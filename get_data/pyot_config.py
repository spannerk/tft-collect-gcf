import os
from pyot.conf.model import activate_model, ModelConf
from pyot.conf.pipeline import activate_pipeline, PipelineConf

@activate_model("tft")
class TFTModel(ModelConf):
    default_platform = "euw1"
    default_region = "europe"
    default_version = "latest"
    default_locale = "en_us"

@activate_pipeline("tft")
class TFTPipeline(PipelineConf):
    name = "tft_main"
    default = True
    stores = [
        {
            "backend": "pyot.stores.omnistone.Omnistone"
            ,
            "expirations": {
            }
        },
        {
            "backend": "pyot.stores.cdragon.CDragon",
        },
        {
            "backend": "pyot.stores.riotapi.RiotAPI",
            "api_key": os.environ["RIOT_API_KEY"],
        }
    ]