from hub_connector import hub_connector
from NMFhierarchy import NMFhierarchy

class nmf_analyzer:
    """ Class to analyze the NMF hierarchy and check its integrity.
    This class provides methods to print the NMF hierarchy and check its integrity."""


    def __init__(self, hub: hub_connector):
        self.hub = hub
        self.hierarchy = NMFhierarchy(hub)

        self._output_lines = []
        self.print_output = False


    def print_indent(self, msg : str = "", indent: int = 0 , alert: bool = False):
        """
        Prints msg with indentation and optional alert formatting.
        If collect is True, appends the output to self._output_lines instead of printing.
        """
        RED_string = "\033[91m"  # Red color for alert
        Black_string = "\033[0m"  # Reset color to default

        indent_str = " " * indent + msg

        if alert and self.print_output:
            # If alert is True, color the message red for jupyter terminal output
            # if msg is collected for html do not use ESC sequences
            indent_str =  RED_string + indent_str + Black_string

        if self.print_output:
            print(indent_str)
        else:
            self._output_lines.append(indent_str)

    def reset_output(self):
        """Resets the collected output lines."""
        self._output_lines = []

    def get_output(self):
        """Returns all collected output as a single string."""
        return "\n".join(self._output_lines)

    def print_nmf_hierarchy(self, print_output : bool = True):
        """
        Prints the hierarchy:
        - For each location node, print the abstraction and distribution type nodes below
        - For each abstraction/distribution node, print the modules below
        - For each module node, print the assets below
        - Indent each level by 5 spaces
        """

        self.reset_output()
        self.print_output = print_output
        from datetime import datetime
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.print_indent(f"Printing NMF hierarchy for user {self.hub.username} at time {now_str} ...", indent=0)
        for location in self.hierarchy.get_locations():
            self.print_indent(f"{location}")
            for subnode in location.subnodes:
                if subnode.type in ("water_abstraction", "water_distribution"):
                    self.print_indent(f"{subnode}", indent=5)
                    for module in subnode.subnodes:
                        self.print_indent(f"{module}", indent=10)
                        for instr in getattr(module, 'instrumentations', []):
                            self.print_indent(f"{instr}", indent=15)
                            for val in getattr(instr, 'value_keys', []):
                                self.print_indent(f"Value Key: {val}, Thresholds: {instr.thresholds.get(val, []) }", indent=20)
                            for asset in getattr(instr, 'assets', []):
                                self.print_indent(f"{asset}", indent=20)
        
        
        return self.get_output()

    def check_non_empty_elems(self, elems : list, error_msg:str, indent: int = 0):
        """
        Check if the given list of elements is non-empty.
        If empty, prints an error message and returns False.
        """
        if not elems:
            self.print_indent(error_msg, indent=indent, alert=True)
            return False
        return True


    def check_nmf_integrity(self, print_output: bool = True):
        """
        Check if all NMF objects are linked correctly and have valid attributes.
        - There should be at least one NMFnode with type 'location'.
        - Each location needs to have at least one application of type water_abstraction/water_distribution node.
        - Each module should have at least one NMFinstrumentation.
        - Each NMFinstrumentation should have at least one NMFasset.
        - Each NMFinstrumentation should have a primary value key specification.
        - Each NMFinstrumentation should have at least one value key/values.
        - depending on the type of NMFinstrumentation, it should have specific value keys and thresholds.
        """

        indent = 0
        self.reset_output()
        self.print_output = print_output
        from datetime import datetime
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.print_indent(f"Checking integrity of NMF hierarchy for user {self.hub.username} at time {now_str}...", indent=indent)

        self.print_indent("Checking locations ...", indent=indent)
        locations = self.hierarchy.get_locations()
        if not self.check_non_empty_elems(locations, "No locations found in the NMF hierarchy.", indent=indent):
            return
        
        for loc in locations:
            self.check_location(loc, indent=indent+5)
        self.print_indent("Locations checked.", indent=indent)

        self.print_indent("NMF integrity check completed.", indent=indent)

        return self.get_output()

    def check_location(self, location, indent=0):

        self.print_indent(f"Checking applications for location {location} ...", indent=indent)        
        apps = self.hierarchy.get_applications(location)
        if not self.check_non_empty_elems(apps, f"Location {location} has no water_abstraction or water_distribution nodes.", indent=indent):
            return

        for app in apps:
            self.check_application(app, indent=indent+5)
        self.print_indent("Applications checked.", indent=indent)

    def check_application(self, app, indent=0):

        self.print_indent(f"Checking modules for application {app} ...", indent=indent)
        modules = self.hierarchy.get_modules(app)
        if not self.check_non_empty_elems(modules, f"Application {app} has no modules.", indent=indent):
            return

        for module in modules:
            self.check_module(module, indent=indent+5)
        self.print_indent("Modules checked.", indent=indent)

    def check_module(self, module, indent=0):

        self.print_indent(f"Checking instrumentations for module {module} ...", indent=indent)
        instrs = self.hierarchy.get_instrumentations(module)
        if not self.check_non_empty_elems(instrs, f"Module {module} has no instrumentations.", indent=indent):
            return
        
        for instr in instrs:
            self.check_instrumentation(instr, indent=indent+5)
        self.print_indent("Instrumentations checked.", indent=indent)

    def check_instrumentation(self, instr, indent=0):

        if not self.check_non_empty_elems(instr.assets, f"Instrumentation {instr} has no assets.", indent=indent):
            return

        if instr.type == "undefined":
            self.print_indent(f"Instrumentation {instr} has type 'undefined'.", indent=indent, alert=True)

        if instr.primary_val_key is None:
            self.print_indent(f"Instrumentation {instr} has no primary value key specification.", indent=indent, alert=True)

        if not instr.value_keys:
            self.print_indent(f"Instrumentation {instr} has no value keys/values.", indent=indent, alert=True)

        if instr.type == "flow":
            if not "totalizer1" in instr.value_keys:
                self.print_indent(f"Instrumentation {instr} of type 'flow' has no 'totalizer1' value key.", indent=indent, alert=True)

            if not "volumeflow" in instr.value_keys:
                self.print_indent(f"Instrumentation {instr} of type 'flow' has no 'volumeflow' value key.", indent=indent, alert=True)
            else:
                limits = { type: (name,val) for (name,type,val) in instr.thresholds.get("volumeflow", {})}
                if not limits.get("upper", None  ):
                    self.print_indent(f"Instrumentation {instr} of type 'flow' has no upper threshold for 'volumeflow'.", indent=indent, alert=True)

        if instr.type == "pressure" or instr.type == "analysis":
            self.print_indent(f"Checking thresholds for instrumentation {instr} of type '{instr.type}' ...", indent=indent)
            for k in instr.value_keys:
                limits = { type: (name,val) for (name,type,val) in instr.thresholds.get(k, {})}
                                        
                if not limits.get("upper", None):
                    self.print_indent(f"Instrumentation {instr} of type '{instr.type}' has no upper threshold for '{k}'.", indent=indent, alert=True)
                
                if not limits.get("lower", None):
                    self.print_indent(f"Instrumentation {instr} of type '{instr.type}' has no lower threshold for '{k}'.", indent=indent, alert=True)

        if instr.type == "pump":
            if not "individual_pump_on" in instr.value_keys:
                self.print_indent(f"Instrumentation {instr} of type 'pump' has no 'individual_pump_on' value key.", indent=indent, alert=True)  
