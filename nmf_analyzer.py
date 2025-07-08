from arrow import get
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

        if alert:
            if self.print_output:
                # If alert is True, color the message red for jupyter terminal output
                # if msg is collected for html do not use ESC sequences
                indent_str =  RED_string + indent_str + Black_string
            else:
                indent_str = " " * indent + "  WARNING: " + msg

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
        # Statistics counters
        n_locations = 0
        n_apps = 0
        n_modules = 0
        n_instrs = 0
        n_assets = 0
        instr_type_counts = {}
        app_type_counts = {}
        module_type_counts = {}

        for location in self.hierarchy.get_locations():
            n_locations += 1
            self.print_indent(f"{location}")
            for subnode in location.subnodes:
                if subnode.type in ("water_abstraction", "water_distribution"):
                    n_apps += 1
                    app_type = getattr(subnode, 'type', 'undefined')
                    app_type_counts[app_type] = app_type_counts.get(app_type, 0) + 1
                    self.print_indent(f"{subnode}", indent=5)
                    for module in subnode.subnodes:
                        n_modules += 1
                        module_type = getattr(module, 'type', 'undefined')
                        module_type_counts[module_type] = module_type_counts.get(module_type, 0) + 1
                        self.print_indent(f"{module}", indent=10)
                        for instr in getattr(module, 'instrumentations', []):
                            n_instrs += 1
                            instr_type = getattr(instr, 'type', 'undefined')
                            instr_type_counts[instr_type] = instr_type_counts.get(instr_type, 0) + 1
                            self.print_indent(f"{instr}", indent=15)
                            for val in getattr(instr, 'value_keys', []):
                                self.print_indent(f"Value Key: {val}, Thresholds: {instr.thresholds.get(val, []) }", indent=20)
                            for asset in getattr(instr, 'assets', []):
                                n_assets += 1
                                self.print_indent(f"{asset}", indent=20)

        # Print statistics summary
        self.print_indent("---", indent=0)
        self.print_indent(f"Statistics:", indent=0)
        self.print_indent(f"Locations: {n_locations}", indent=0)
        self.print_indent(f"Applications: {n_apps}", indent=0)
        for app_type, count in sorted(app_type_counts.items()):
            self.print_indent(f"  {app_type}: {count}", indent=2)
        self.print_indent(f"Modules: {n_modules}", indent=0)
        for module_type, count in sorted(module_type_counts.items()):
            self.print_indent(f"  {module_type}: {count}", indent=2)
        self.print_indent(f"Instrumentations: {n_instrs}", indent=0)
        for instr_type, count in sorted(instr_type_counts.items()):
            self.print_indent(f"  {instr_type}: {count}", indent=2)
        self.print_indent(f"Assets: {n_assets}", indent=0)

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


    def group_instr_by_latest_values(self):
        """
        Groups all instrumentations in the hierarchy by the recency of their latest measurement value.
        Returns three lists of tuples (instr, latest_timestamp):
            - i_24: instrumentations with at least one value <24h old
            - i_24_72: instrumentations with no value <24h but at least one <72h old
            - i_72: instrumentations with no value <72h old
        Also returns a dict latest_instr_values (currently unused) for possible future extension.
        Timestamps are handled as pandas Timestamps and compared in UTC.
        """

        instrumentations = list(self.hierarchy.nmf_instrumentations.values())
        latest_instr_values = {}

        i_24= []
        i_24_72 = []
        i_72 = []

        for instr in instrumentations:
            cmd = f"instrumentations/{instr.id}/values"
            latest_values = self.hub.call_hub(cmd=cmd).get("values", [])

            if not latest_values: continue

            import pandas as pd
            latest_timestamps = [ pd.to_datetime(latest_values.get("timestamp"))
                                 for latest_values in latest_values 
                                 if latest_values.get("timestamp")]
            latest_instr_ts = max(latest_timestamps)
           #print(f"Found {latest_instr_ts} for instrumentation {inst}")

            import datetime
            now = pd.Timestamp.now().tz_localize('UTC')
            
            age = now - latest_instr_ts
            #print(f"Instrumentation {instr} latest value timestamp: {latest_instr_ts}, age: {age}")
            if age < pd.Timedelta(hours=24):
                i_24.append((instr, latest_instr_ts))
            elif age < pd.Timedelta(hours=72):
                i_24_72.append((instr, latest_instr_ts))
            else:
                i_72.append((instr, latest_instr_ts))

        return i_24, i_24_72, i_72, latest_instr_values

    def analyse_instr_timeseries(self, print_output: bool = True):
        """
        Analyzes all instrumentations in the hierarchy and reports the recency of their measurement entries by value key.
        For each instrumentation, groups value keys into:
            - those with at least one measurement in the last 24 hours
            - those with no measurement in the last 24 hours but at least one in the last 72 hours
            - those with no measurement in the last 72 hours
        Uses get_grouped_value_keys for grouping and prints a summary for each group.
        """

        self.reset_output()
        self.print_output = print_output

        from datetime import datetime
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        i_24, i_24_72, i_72, latest_instr_values = self.group_instr_by_latest_values()
        self.print_indent(f"Analysing timeseries data at {now_str} for {len(i_24) + len(i_24_72) + len(i_72)} instrumentations...")

        self.print_indent(f"  Instrumentations with at least one measurement entry younger than 24h:")
        for (instr, latest_instr_ts) in i_24:
            self.print_indent(f"     {instr} (latest timestamp: {latest_instr_ts})")

        self.print_indent(f"  Instrumentations with no measurement entry younger than 24h but at least one younger than 72h:")
        for (instr, latest_instr_ts) in i_24_72:
            self.print_indent(f"     {instr} (latest timestamp: {latest_instr_ts})")

        self.print_indent(f"  Instrumentations with no measurement entry younger than 72h:")
        for (instr, latest_instr_ts) in i_72:
            self.print_indent(f"     {instr} (latest timestamp: {latest_instr_ts})")

        return self.get_output()


           