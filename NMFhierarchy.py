from dataclasses import dataclass, field
from typing import Optional

from hub_connector import hub_connector  

@dataclass
class NMFhierarchy:

    """Class to hold the NMF hierarchy and its elements."""

    hub: hub_connector
    nmf_nodes: dict[int, "NMFnode"]
    nmf_instrumentations: dict[int, "NMFinstrumentation"]
    nmf_assets: dict[int, "NMFasset"]

    def __init__(self, hub: hub_connector):
        self.hub = hub
        self.nmf_nodes = {}
        self.nmf_instrumentations = {}
        self.nmf_assets = {}
        self.clone_hierarchy()

    def clone_hierarchy(self):
        #  logic for cloning hierarchy
        nodes = self.get_node_info()
        instrumentations, assets = self.get_instrumentation_info()

        self.create_nmf_objects(nodes, instrumentations, assets)
        self.create_links(nodes, instrumentations, assets)

    def create_nmf_objects(self, nodes, instrumentations, assets):
        # creating NMF objects
        # Create NMF nodes, instrumentations, and assets
        for node_id, node in nodes.items():
            nmf_node = NMFnode(
                name=node["name"],
                type=node["type"],
                id=node["id"],
                subnodes=[],
                parent=node.get("parent_id"),
                instrumentations=[]
            )
            self.nmf_nodes[node_id] = nmf_node

        for instr_id, instr in instrumentations.items():
            nmf_instr = NMFinstrumentation(
                tag=instr["tag"],
                type=instr["type"],
                id=instr["id"],
                primary_val_key=instr.get("specifications"),
                value_keys=instr.get("value_keys", []),
                parent=None,
                assets=instr["assets"],
                thresholds={ k :  [ (t.get("name"), t.get("threshold_type"), t.get("value")) 
                                   for t in instr["thresholds"] if t.get("key") == k] 
                            for k   in instr.get("value_keys", [])},
                nodes=[]
            )

            self.nmf_instrumentations[instr_id] = nmf_instr

        for asset_id, asset in assets.items():
            nmf_asset = NMFasset(
                serial_number=asset["serial"],
                prod_code=asset["prod_code"],
                prod_name=asset["product_name"],
                id=asset_id,
                instrumentations=[]
            )
            self.nmf_assets[asset_id] = nmf_asset

    def create_links(self, nodes, instrumentations, assets):
        # creating links between NMF objects

        # setting sub_nodes & instrumentations for NMF nodes
        for node_id, node in nodes.items():
            pid = node.get("parent_id")
            nmf_node = self.nmf_nodes[node_id]
            if pid is not None:
                parent_node = self.nmf_nodes.get(pid)
                parent_node.subnodes.append(nmf_node)

            nmf_node.instrumentations = [self.nmf_instrumentations[instr_id]
                                         for instr_id in node["instrumentations"]]

        # setting assets for NMF instrumentations
        for instr_id, instr in instrumentations.items():
            nmf_instr = self.nmf_instrumentations[instr_id]
            nmf_instr.assets = [self.nmf_assets[a["id"]] for a in instr["assets"]]


        # setting instrumentations for NMF assets
        for instr in self.nmf_instrumentations.values():
            for a in instr.assets:
                a.instrumentations.append(instr)               

    def get_node_info(self):
        # retrieving node information
        cmd = "nodes?include=type%2Cinstrumentations%2Cinstrumentations.type%2Cparent%2Cparent.type"
        response = self.hub.call_hub_pagination(cmd=cmd, next_key="nodes")
        nodes = dict()

        for node in response:
            node_id = node["id"]
            nodes[node_id] = {
                "name": node["name"],
                "type": node["type"]["code"],
                "id": node_id,
                "parent_id" : node.get("parent", {}).get("id"),
                "instrumentations" : [i["id"] for i in node["instrumentations"]["items"]]       
            }
        
        return nodes
   
    def get_instrumentation_info(self):
        # retrieving instrumentation information

        cmd="instrumentations?include=type%2Cassets%2Cassets.product%2Cparent%2Cspecifications%2Cvalues%2Cthresholds"
        response = self.hub.call_hub_pagination(cmd=cmd, next_key="instrumentations")
        instrumentations = dict()
        assets = dict()

        for instr in response:
            instr_id = instr["id"]

            asset_list = [
                dict(
                    id=a["id"],
                    serial=a["serial_number"],
                    product_name=a["product"].get("name", "n.a."),
                    prod_code=a["product"].get("product_code", "n.a.")
                )
                for a in instr["assets"]["items"]
            ]
            instrumentations[instr_id] = {
                "tag": instr["tag"],
                "type": instr["type"]["code"],
                "id": instr_id,
                "assets": asset_list,
                "specifications": instr.get("specifications", {}).get("eh_nni_primary_key", {}).get("value"),
                "value_keys":[vk.get("key") for vk in [k for k in instr.get("values", {})]],
                "thresholds": instr.get("thresholds", {}).get("items", [])
            }

            for ai in asset_list:
                assets[ai["id"]] = ai
            
        return instrumentations, assets
    
    #################################################
    # getters for NMF objects
    #################################################

    def get_locations(self):
        """Return all NMFnode objects with type == 'location'."""
        return [node for node in self.nmf_nodes.values() if node.type == "location"]
    
    def get_applications(self, location):
        """Return all NMFnode objects with type == 'water_application'."""
        return [node for node in location.subnodes if node.type in ("water_abstraction", "water_distribution")]
    
    def get_modules(self, water_app):
        """Return all Module objects for WATER APP."""
        return water_app.subnodes 
    
    def get_instrumentations(self, module):
        """Return all NMFinstrumentation objects for a given module."""
        return module.instrumentations 
    
    def get_assets(self, instrumentation):
        """Return all NMFasset objects for a given instrumentation."""
        return instrumentation.assets 
    
    def get_asset_by_serial(self, serial):
        """Return NMFasset object by serial number."""
        for asset in self.nmf_assets.values():
            if asset.serial_number == serial:
                return asset
        return None
    


@dataclass
class NMFelement:
    id: int
    """Base class for all NMF objects."""
    def __post_init__(self):
        if not isinstance(self.id, int) or self.id < 0:
            raise ValueError("id must be a non-negative integer")

@dataclass
class NMFnode(NMFelement):
    name: str
    type: str
    subnodes: list["NMFnode"] = field(default_factory=list)
    parent: Optional["NMFnode"] = None
    instrumentations: list["NMFinstrumentation"] = field(default_factory=list)

    def __str__(self):
        return f"node({self.id}, '{self.name}', {self.type})"

@dataclass
class NMFinstrumentation(NMFelement):
    tag: str
    type: str
    parent: Optional["NMFnode"] = None
    assets: list["NMFasset"] = field(default_factory=list)
    nodes: list["NMFnode"] = field(default_factory=list)
    primary_val_key: Optional[str] = None
    value_keys: list[str] = field(default_factory=list)
    thresholds: list[dict] = field(default_factory=list)

    def __str__(self):
        return f"instr({self.id}, '{self.tag}', {self.type}, '{self.primary_val_key}')"

@dataclass
class NMFasset(NMFelement):  
    serial_number: str
    prod_code: str
    prod_name: str
    instrumentations: list["NMFinstrumentation"] = field(default_factory=list)

    def __str__(self):
        return f"asset({self.id}, '{self.serial_number}', '{self.prod_code}')"
  