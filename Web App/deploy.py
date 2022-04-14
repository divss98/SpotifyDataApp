from flask import Flask,render_template,request
import webbrowser
import insights
import base64
from io import BytesIO
import mysql.connector
from matplotlib.figure import Figure
mydb = mysql.connector.connect(
    host='localhost',
    database='mydatabase',
    user='root',
    password='DSCI551.2021')


def qi(choice,filter):
    mycursor = mydb.cursor()

    if choice=='Artists':
        if filter=='':
            mycursor.execute("select art.artist_name, art.artist_genre, art.artist_followers, art.artist_popularity, art.artist_image, art.artist_homepage from artist art order by art.artist_name")
            field_names = [i[0] for i in mycursor.description]

        else:
            mycursor.execute("select art.artist_name, art.artist_genre, art.artist_followers, art.artist_popularity, art.artist_image, art.artist_homepage from artist art where artist_name like '%"+filter+"%' order by art.artist_name")
            field_names = [i[0] for i in mycursor.description]

    if choice=='Songs':
        query="select t.track_name, art.artist_name, art.artist_genre, alb.album_name, t.track_popularity, aud.track_duration as 'track_duration_ms',round(aud.danceability,3) as 'danceability', round(aud.energy,3) as 'energy', round(aud.loudness,3) as 'loudness',round(aud.speechiness,3) as 'speechiness', round(aud.acousticness,3) as 'acousticness', round(aud.instrumentalness,3) as 'instrumentalness', round(aud.liveness,3) as 'liveness', round(aud.valence,3) as 'valence',  t.track_homepage from track t left join audio aud on aud.track_id = t.track_id left join album alb on alb.album_id = t.track_album_id left join artist art on art.artist_id = t.track_artist_id"
        if filter=='':
            mycursor.execute(query+" order by t.track_name")
            field_names = [i[0] for i in mycursor.description]

        else:
            mycursor.execute(query+" where t.track_name like '%"+filter+"%' order by t.track_name")
            field_names = [i[0] for i in mycursor.description]

    if choice=='Album':
        query="select alb.album_name, art.artist_name, alb.album_release_date, alb.album_total_tracks, art.artist_genre, alb.album_image, alb.album_homepage, art.artist_image, art.artist_homepage from album alb left join artist art on art.artist_id = alb.album_artist_id"
        if filter=='':
            mycursor.execute(query+" order by alb.album_name")
            field_names = [i[0] for i in mycursor.description]

        else:
            mycursor.execute(query+" where album_name like '%"+filter+"%' order by alb.album_name")
            field_names = [i[0] for i in mycursor.description]

    if choice=='Genre':
        query="select art.artist_name, art.artist_genre, art.artist_followers, art.artist_popularity, art.artist_image, art.artist_homepage from artist art where artist_genre like '% "+filter+"%' or artist_genre like '%"+filter +" %' order by art.artist_name"
        mycursor.execute(query)
        field_names = [i[0] for i in mycursor.description]

    myresult = mycursor.fetchall()
    count=mycursor.rowcount
    return myresult,field_names,count


app = Flask(__name__)

@app.route('/')
def index():
    return render_template('home.html')

@app.route('/search_engine')
def search_engine():
    return render_template('search_engine.html')

@app.route('/stats')
def stats():
    x=insights.show_insights()
    path='/static/images/'
    return render_template('stats.html', scatter =path+'scatter.png',bar=path+'barp.png',radar=path+'radar.png')



@app.route('/result', methods = ['POST'])
def result():
    choice=request.form['choice']
    filter=request.form['query']
    if choice=='Artists':
        myresult,field_names,count=qi(choice,filter)
    if choice=='Songs':
        myresult,field_names,count=qi(choice,filter)
    if choice=='Album':
        myresult,field_names,count=qi(choice,filter)
    if choice=='Genre':
        myresult,field_names,count=qi(choice,filter)
    #print(str(count))
    return render_template('search_engine.html',data=myresult,field=field_names,option=choice,count=str(count))




if __name__ == '__main__':
    webbrowser.open('http://localhost:5000')
    app.run()
