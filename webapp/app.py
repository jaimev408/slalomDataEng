import dash
from funcs import makeDFApp, computeQueries, toBucket
from dash.dependencies import Input, Output, State
import dash_html_components as html
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

#Style for the button
BUTTON_STYLE = {
    'textAlign':'center',
    }

#application init
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

#App UI layout code
button = html.Div([html.Button('Start Process!', id='start'),], style=BUTTON_STYLE,)


app.layout = html.Div([html.Div(html.H3("Data Engineering Proof of Concept. Press Button to start!")),
                       button,
                       html.Div(id='tabs-example-content'),
                       ])
#Callback function to process data
#input: button click
#Output: done html message to populate layout
@app.callback(Output('tabs-example-content', 'children'),
              Input('start', 'n_clicks'),)
def startProcess(n_clicks):
    print('clicked')
    df = makeDFApp()
    print('df made')
    m,z,u = computeQueries(df)
    toBucket(df, 'mainDF.gz')
    toBucket(m, 'meanReviewsBus.csv')
    toBucket(z, 'mostActiveUsers.csv')
    toBucket(u, 'topZipReviews.csv')
    return html.Div(html.H2("DONE"))
    
#Main function to run app
if __name__ == '__main__':
    app.run_server(debug=False)