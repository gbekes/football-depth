import os
from typing import Optional

from aswan import ProxyAuth, ProxyBase
from dotenv import load_dotenv

load_dotenv()


class PacketProxy(ProxyBase):

    expiration_secs = float("inf")
    prefix = "http"
    port_no = 31112

    def get_creds(self) -> Optional[ProxyAuth]:
        return ProxyAuth(user=os.environ["PACK_USER"], password=os.environ["PACK_PW"])

    def _load_host_list(self) -> list:
        return ["proxy.packetstream.io"]
