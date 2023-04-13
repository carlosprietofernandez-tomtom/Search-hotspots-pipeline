import plotly.express as px


def compute_plot(df, letter):
    fig = px.scatter(x=df.index, y=df[letter])
    return fig
