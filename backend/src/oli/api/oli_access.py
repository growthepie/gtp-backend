"""Account-wide read limits shared by all API workers through PostgreSQL."""

import os
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from fastapi import HTTPException


def positive_env(name, default):
    value = int(os.getenv(name, str(default)))
    if value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


@dataclass(frozen=True)
class ReadLimits:
    requests_per_minute: int
    addresses_per_month: int
    bulk_addresses: int
    concurrent_reads: int

    @classmethod
    def from_env(cls):
        return cls(
            positive_env("OLI_READ_REQUESTS_PER_MINUTE", 60),
            positive_env("OLI_READ_ADDRESSES_PER_MONTH", 10000),
            positive_env("OLI_READ_BULK_ADDRESSES", 100),
            positive_env("OLI_READ_CONCURRENT_REQUESTS", 5),
        )


@dataclass(frozen=True)
class Reservation:
    lease_id: str
    owner_id: str
    month: datetime
    units: int
    remaining: int
    reset: datetime


class ReadAccess:
    def __init__(self, pool, limits):
        self.pool = pool
        self.limits = limits

    async def reserve(self, owner_id, units):
        """Reserve requested address slots before reading any labels.

        Fixed minute windows and UTC calendar months. Account advisory locks make
        checks atomic across keys, processes and replicas. Leases recover after
        a crashed worker; every DB command has a shorter timeout than the lease.
        """
        if units < 0:
            raise ValueError("Address reservation units cannot be negative")
        lease_id = str(uuid4())
        rejection = None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", owner_id)
                clock = await conn.fetchrow("""
                    SELECT date_trunc('minute', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS minute,
                           date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS month,
                           now() AS current_time
                """)
                minute, month, now = clock["minute"], clock["month"], clock["current_time"]
                reset = (month.replace(year=month.year + 1, month=1) if month.month == 12
                         else month.replace(month=month.month + 1))
                await conn.execute("DELETE FROM public.api_read_leases WHERE owner_id = $1 AND expires_at <= now()", owner_id)
                await conn.execute("""
                    DELETE FROM public.api_read_buckets WHERE owner_id = $1
                    AND ((kind = 'minute' AND period_start < $2::timestamptz - interval '1 hour')
                         OR (kind = 'month' AND period_start < $3::timestamptz - interval '13 months'))
                """, owner_id, minute, month)
                count = await conn.fetchval("""
                    INSERT INTO public.api_read_buckets(owner_id, kind, period_start, used)
                    VALUES ($1, 'minute', $2, 1)
                    ON CONFLICT (owner_id, kind, period_start)
                    DO UPDATE SET used = api_read_buckets.used + 1 RETURNING used
                """, owner_id, minute)
                used = await conn.fetchval("""
                    SELECT used FROM public.api_read_buckets
                    WHERE owner_id = $1 AND kind = 'month' AND period_start = $2
                """, owner_id, month) or 0
                active = await conn.fetchval("SELECT count(*) FROM public.api_read_leases WHERE owner_id = $1", owner_id)
                if count > self.limits.requests_per_minute:
                    rejection = ("rate_limit", max(1, math.ceil((minute + timedelta(minutes=1) - now).total_seconds())))
                elif active >= self.limits.concurrent_reads:
                    rejection = ("concurrent_read_limit", 1)
                elif used + units > self.limits.addresses_per_month:
                    rejection = ("monthly_address_quota", max(1, math.ceil((reset - now).total_seconds())))
                else:
                    if units:
                        await conn.execute("""
                            INSERT INTO public.api_read_buckets(owner_id, kind, period_start, used)
                            VALUES ($1, 'month', $2, $3)
                            ON CONFLICT (owner_id, kind, period_start)
                            DO UPDATE SET used = api_read_buckets.used + EXCLUDED.used
                        """, owner_id, month, units)
                    await conn.execute("""
                        INSERT INTO public.api_read_leases(id, owner_id, expires_at)
                        VALUES ($1::uuid, $2, now() + interval '5 minutes')
                    """, lease_id, owner_id)
        # Raise outside the transaction so rejected requests still consume rate slots.
        if rejection:
            code, retry = rejection
            raise HTTPException(429, detail={"code": code, "usage_url": "/account/usage", "plans_url": "/plans"},
                                headers={"Retry-After": str(retry)})
        return Reservation(lease_id, owner_id, month, units, self.limits.addresses_per_month - used - units, reset)

    async def release(self, reservation, failed=False):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", reservation.owner_id)
                lease = await conn.fetchval("DELETE FROM public.api_read_leases WHERE id = $1::uuid RETURNING id", reservation.lease_id)
                if lease and failed and reservation.units:
                    await conn.execute("""
                        UPDATE public.api_read_buckets SET used = greatest(0, used - $3)
                        WHERE owner_id = $1 AND kind = 'month' AND period_start = $2
                    """, reservation.owner_id, reservation.month, reservation.units)

    async def usage(self, owner_id):
        async with self.pool.acquire() as conn:
            used = await conn.fetchval("""
                SELECT used FROM public.api_read_buckets WHERE owner_id = $1 AND kind = 'month'
                AND period_start = date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
            """, owner_id) or 0
        return {"address_slots_used": used, "address_slots_remaining": max(0, self.limits.addresses_per_month - used),
                "limits": self.limits.__dict__, "period": "UTC calendar month", "plans_url": "/plans"}
