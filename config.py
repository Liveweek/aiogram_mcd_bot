import os


TG_TOKEN = os.getenv("TG_TOKEN")
ADMIN_IDS = {
    267173666
}

button_help_text = 'Помощь'
button_request_errors_text = 'Errors'
button_request_status_text = 'Full stat'
button_request_rtp_text = 'Short'
button_request_vf_text = 'Long'
button_show_errors = 'Failed Resources'
button_show_resources = 'Show Resources'


DATABASELINK_TEST = "postgres://postgres:Orion123@localhost:5432/postgres"
DATABASELINK_POSTGRES = os.getenv("PG_URL")
# DATABASELINK_POSTGRES = "postgres://etl_cfg:Orion123@10.252.151.9:5452/postgres"
DATABASELINK_ETL = "postgres://etl_cfg:Orion123@10.252.151.3:5452/etl"
