from pydantic import BaseSettings


class Settings(BaseSettings):
    title: str = "signal_copier"
    url: str = "http://rfx6.com/rfx6_copier/web_interface"
    provider_not_found: int = -10
    client_not_found: int = -11
    ticket_already_exist: int = -20
    authentication_failed: int = -30
    parent_ticket_not_found: int = -40
    deal_is_repeated: int = -50
    client_no_provider_found: int = -60
    no_new_deal: int = -70
    provider_deal_not_found: int = -80
    new_order: int = 1
    close_order: int = 2
    delete_order: int = 3
    modify_order: int = 4
    sub_order: int = 5


settings = Settings()
