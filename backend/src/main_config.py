from pydantic import BaseModel, Field, field_validator, ValidationInfo
from typing import Optional, List, Dict, Any
from src.db_connector import DbConnector
import zipfile
import io
import json
import requests
import pickle

class MainConfig(BaseModel):
    origin_key: str
    chain_type: str
    l2beat_stage: str = "NA" ## default NA
    maturity: str = "NA" ## default NA
    caip2: Optional[str] = None
    evm_chain_id: Optional[int] = None
    name: str
    name_short: str
    company: Optional[str] = None
    bucket: str
    block_explorers: Optional[dict] = None
    colors: dict
    logo: dict = {
        'body': "<svg width=\"15\" height=\"15\" viewBox=\"0 0 15 15\" fill=\"none\" xmlns=\"http://www.w3.org/2000/svg\"><circle cx=\"7.5\" cy=\"7.5\" r=\"7.5\" fill=\"currentColor\"/></svg>",
        'width': 15,
        'height': 15
    }
    ecosystem: list = Field(alias="ecosystem_old")    

    ## API
    api_in_main: bool = Field(alias="api_in_api_main", default=False)
    api_in_fees: Optional[bool] = Field(alias="api_in_api_fees", default=None)
    api_in_economics: Optional[bool] = Field(alias="api_in_api_economics", default=None)
    api_in_labels: Optional[bool] = Field(alias="api_in_api_labels", default=None)
    api_in_apps: Optional[bool] = Field(alias="api_in_api_apps", default=None)
    api_deployment_flag: Optional[str] = Field(alias="api_api_deployment_flag", default=None)
    api_exclude_metrics: Optional[List[str]] = Field(alias="api_api_exclude_metrics", default=None)
    api_tab_status: Optional[dict] = Field(alias="api_tab_status", default=None)

    ## ALIASES
    aliases_l2beat: Optional[str] = Field(alias="aliases_l2beat", default=None)
    aliases_l2beat_slug: Optional[str] = Field(alias="aliases_l2beat_slug", default=None)
    aliases_coingecko: Optional[str] = Field(alias="aliases_coingecko", default=None)
    aliases_rhino: Optional[str] = Field(alias="aliases_rhino", default=None)
    aliases_blockscout_url: Optional[str] = Field(alias="aliases_blockscout_url", default=None)
    aliases_defillama: Optional[str] = Field(alias="aliases_defillama", default=None)

    ## METADATA
    metadata_description: Optional[str] = Field(alias="metadata_description", default=None)
    metadata_symbol: Optional[str] = Field(alias="metadata_symbol", default=None)
    metadata_launch_date: Optional[str] = Field(alias="metadata_launch_date", default=None)
    metadata_da_layer: Optional[str] = Field(alias="metadata_da_layer", default=None)
    metadata_technology: Optional[str] = Field(alias="metadata_technology", default=None)
    metadata_purpose: Optional[str] = Field(alias="metadata_purpose", default=None)
    metadata_stack: Optional[Dict] = Field(alias="metadata_stack", default=None)
    metadata_raas: Optional[str] = Field(alias="metadata_raas", default=None)

    ## SOCIALS
    socials_website: Optional[str] = Field(alias="socials_website", default=None)
    socials_twitter: Optional[str] = Field(alias="socials_twitter ", default=None)

    ## RUNS
    runs_aggregate_blockspace: Optional[bool] = Field(alias="runs_aggregate_blockspace", default=None)
    runs_aggregate_addresses: Optional[bool] = Field(alias="runs_aggregate_addresses ", default=None)
    runs_aggregate_apps: Optional[bool] = Field(alias="runs_aggregate_apps", default=None)
    runs_contract_metadata: Optional[bool] = Field(alias="runs_contract_metadata", default=None)

    ## RPC CONFIG
    backfiller_on: Optional[bool] = Field(alias="backfiller_backfiller_on", default=None)
    backfiller_batch_size: Optional[int] = Field(20, alias="backfiller_batch_size")

    ## CROSS CHECK
    cross_check_url: Optional[str] = Field(alias="cross_check_url", default=None)
    cross_check_type: Optional[str] = Field(alias="cross_check_type", default=None)

    ## CIRCULATING SUPPLY
    cs_token_address: Optional[str] = Field(alias="circulating_supply_token_address", default=None)
    cs_deployment_date: Optional[str] = Field(alias="circulating_supply_token_deployment_date", default=None)
    cs_deployment_origin_key: Optional[str] = Field(alias="circulating_supply_token_deployment_origin_key", default=None)
    cs_supply_function: Optional[str] = Field(alias="circulating_supply_token_supply_function", default=None)

    @field_validator('logo', 'l2beat_stage', mode='before')
    @classmethod
    def set_default_if_none(cls, v, info: ValidationInfo):
        if v is None:
            return cls.model_fields[info.field_name].default
        return v

def get_main_config_dict():
    # Get the repository
    repo_url = "https://github.com/growthepie/gtp-dna/tree/main/"
    _, _, _, owner, repo_name, _, branch, *path = repo_url.split('/')

    # Download directory as ZIP file
    zip_url = f"https://github.com/{owner}/{repo_name}/archive/{branch}.zip"
    response = requests.get(zip_url)
    zip_content = io.BytesIO(response.content)

    main_config_dict = []

    with zipfile.ZipFile(zip_content) as zip_ref:
        root_path = 'gtp-dna-main/chains/'
        nameslist = zip_ref.namelist()
        nameslist = [name for name in nameslist if name.startswith(root_path)]

        ## only exact folders from nameslist
        chains = [name.split('/')[2] for name in nameslist if name.endswith('/')]
        chains = [chain for chain in chains if chain]

        for chain in chains:      
            chain_data = {}
            path = f"{root_path}{chain}"

            main_path = f"{path}/main.json"
            logo_path = f"{path}/logo.json"

            if main_path in nameslist:
                with zip_ref.open(main_path) as file:
                    content = file.read().decode('utf-8')
                    content = json.loads(content)
                    chain_data = content
                    
                if logo_path in nameslist:
                    with zip_ref.open(logo_path) as file:
                        content = file.read().decode('utf-8')
                        content = json.loads(content)
                        chain_data["logo"] = content

                main_config_dict.append(chain_data)

    return main_config_dict

def get_main_config(db_connector:DbConnector=None, main_config_dict=None, source='s3', api_version = 'v1'):
    if source == 's3':
        response = requests.get(f"https://api.growthepie.xyz/{api_version}/main_conf.pkl")
        main_config = pickle.loads(response.content)
    elif source == 'github':
        if not main_config_dict:
            main_config_dict = get_main_config_dict()

        main_config = [MainConfig(**data) for data in main_config_dict]

        ## add dynamic l2beat stage info
        stages = db_connector.get_stages_dict()
        ## iterate over stages and update the new_config with the stage
        for stage in stages:
            chain = [chain for chain in main_config if chain.origin_key == stage]
            if chain:
                chain[0].l2beat_stage = stages[stage]

        maturity_levels = db_connector.get_maturity_dict()
        ## iterate over maturity levels and update the new_config with the stage
        for maturity in maturity_levels:
            chain = [chain for chain in main_config if chain.origin_key == maturity]
            if chain:
                chain[0].maturity = maturity_levels[maturity]
    else:
        raise NotImplementedError(f"Source {source} not implemented")

    return main_config

def get_all_l2_config():
    main_config = get_main_config()
    all_l2_config = main_config + [MainConfig(origin_key='all_l2s', chain_type = '-', name='All L2s', name_short='-', bucket='-', colors={"light":["#FFDF27","#FE5468"],"dark":["#FFDF27","#FE5468"],"darkTextOnBackground": False}, ecosystem_old=["op-stack", "op-super", "all-chains"], api_in_api_main=True, api_api_deployment_flag='PROD', api_api_exclude_metrics=[])] ## for multi-chain metrics
    return all_l2_config

def get_multi_config():
    all_l2_config = get_all_l2_config()
    multi_config = all_l2_config + [MainConfig(origin_key='multiple', chain_type = 'all-L2s', name='Multiple L2s', name_short='-', colors = {"light":["#cdd8d3","#cdd8d3"],"dark":["#cdd8d3","#cdd8d3"],"darkTextOnBackground": False} , bucket='-', ecosystem_old=[], api_in_api_main=True, api_api_deployment_flag='PROD', api_api_exclude_metrics=[])] ## for multi-chain metrics
    return multi_config