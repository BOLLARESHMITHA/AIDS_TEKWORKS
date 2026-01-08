import pandas as pd
import streamlit as st
import seaborn as sns

#page configuration

st.set_page_config('Linear Regression app',layout='centered')

def load_csv():
    return sns.load_dataset('taxis')

df=load_csv()

st.markdown('<div>',unsafe_allow_html=True)
st.subheader('Taxis data')
st.dataframe(df.head(10))
st.markdown('</div>',unsafe_allow_html=True)

