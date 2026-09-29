from pydantic import BaseModel, HttpUrl, Field, field_validator, ValidationInfo
from typing import Optional, List
from src.gtp_dna import get_gtp_dna_archive
import json
import requests
import pickle

class DAConfig(BaseModel):
    origin_key: str
    name: str
    name_short: str
    block_explorers: Optional[dict] = None

    colors: dict
    logo: dict = {
        'body': "<svg width=\"15\" height=\"15\" viewBox=\"0 0 15 15\" fill=\"none\" xmlns=\"http://www.w3.org/2000/svg\"><circle cx=\"7.5\" cy=\"7.5\" r=\"7.5\" fill=\"currentColor\"/></svg>",
        'width': 15,
        'height': 15
    } 

    ## API
    incl_in_da_overview: bool = Field(alias="incl_in_da_overview", default=False)
    api_in_main: bool = Field(alias="api_in_api_main", default=False)
    api_exclude_metrics: Optional[List[str]] = Field(alias="api_api_exclude_metrics", default=None)
   
    ## METADATA
    parameters: Optional[dict] = None

    ## SOCIALS
    socials_website: Optional[HttpUrl] = Field(alias="socials_website", default=None)
    socials_twitter: Optional[HttpUrl] = Field(alias="socials_twitter ", default=None)

    ## VALIDATOR to set default values if field exists with None in dictionary
    @field_validator('logo', mode='before')
    @classmethod
    def set_default_if_none(cls, v, info: ValidationInfo):
        if v is None:
            return cls.model_fields[info.field_name].default
        return v

def get_da_config_dict():
    da_config_dict = []

    with get_gtp_dna_archive() as zip_ref:
        root_path = f"{zip_ref.namelist()[0].split('/')[0]}/da_layers/"
        nameslist = zip_ref.namelist()
        nameslist = [name for name in nameslist if name.startswith(root_path)]

        da_layers = [name.split('/')[-2] for name in nameslist
                     if name.endswith('/main.json') and name.count('/') == root_path.count('/') + 1]

        for da_layer in da_layers:      
            da_layer_data = {}
            path = f"{root_path}{da_layer}"

            main_path = f"{path}/main.json"
            logo_path = f"{path}/logo.json"

            if main_path in nameslist:
                with zip_ref.open(main_path) as file:
                    content = file.read().decode('utf-8')
                    content = json.loads(content)
                    da_layer_data = content
                    
                if logo_path in nameslist:
                    with zip_ref.open(logo_path) as file:
                        content = file.read().decode('utf-8')
                        content = json.loads(content)
                        da_layer_data["logo"] = content

                da_config_dict.append(da_layer_data)

    return da_config_dict

def get_da_config(da_config_dict=None, source='s3', api_version = 'v1'):
    if source == 's3':
        response = requests.get(f"https://api.growthepie.xyz/{api_version}/da_conf.pkl")
        da_config = pickle.loads(response.content)
    elif source == 'github':
        if not da_config_dict:
            da_config_dict = get_da_config_dict()

        da_config = [DAConfig(**data) for data in da_config_dict]
    else:
        raise NotImplementedError(f"Source {source} not implemented")

    return da_config
