import streamlit as st
import streamlit.components.v1 as components
from hub_connector import hub_connector, credential
from nmf_analyzer import nmf_analyzer

st.set_page_config(page_title="Netilion Water Analyzer", page_icon=":sun_with_face:", layout="wide")

st.title("Very cool Netilion Water Analyzer (last update 23-07-2025)")

st.header("Enter Credentials & Region")
col1, col2 = st.columns(2)

with col1:
    user = st.text_input("Technical User")
    pwd = st.text_input("Password", type="password")
    api_key = st.text_input("API Key", type="password")

with col2:
    region = st.selectbox("Netilion Region", options=["Global", "India", "Staging"], index=1)
st.markdown("---")

col3, col4, col5 = st.columns(3)
with col3:
    run_structure = st.button("Run structure analysis")

with col4:
    run_consistency = st.button("Run integrity check")

with col5:
    run_recency = st.button("Run recency check")


if not all([user, pwd, api_key]):
    st.error("Please fill in all required fields.")
else:
    try:
        # Create a credential object (assuming you have a Credential class)
        cred = credential(user=user, pwd=pwd, api_key=api_key, production_region=region)
    except Exception as e:
        # authentication failed
        st.text_area("***Output***", value=f"OOPS, some error occured:\n\n Authentication failed: {str(e)}", height=300)
    else:
        hub = hub_connector(credential=cred)
        analyzer = nmf_analyzer(hub)
        # Run your analysis logic here
        output = ""
        if run_structure:
            output = analyzer.print_nmf_hierarchy(print_output=False)  
        elif run_consistency:
            output = analyzer.check_nmf_integrity(print_output=False)
        elif run_recency:
            output = analyzer.analyse_instr_timeseries(print_output=False)


        response = st.text_area("Output", value=output, height=800, label_visibility="hidden")


