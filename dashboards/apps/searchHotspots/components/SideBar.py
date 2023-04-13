import streamlit as st


def SideBar():
    sidebar = st.sidebar

    letter = sidebar.selectbox("Select letter:", ("A", "B", "C", "D"))

    return letter
