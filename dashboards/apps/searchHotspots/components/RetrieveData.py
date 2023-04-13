import numpy as np
import pandas as pd
import streamlit as st


@st.cache()
def load_data():
    t = pd.date_range("1/1/2000", periods=10)
    df = pd.DataFrame(np.random.randn(10, 4), index=t, columns=list("ABCD"))
    df = df.cumsum()
    return df
