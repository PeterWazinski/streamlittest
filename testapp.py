import streamlit as st

st.title("Simple Sum Calculator")

num1 = st.number_input("Enter first number", min_value=0, step=1)
num2 = st.number_input("Enter second number", min_value=0, step=1)

if st.button("Compute Sum"):
    result = num1 + num2
    st.success(f"The sum is: {result}")
