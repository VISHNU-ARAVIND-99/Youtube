import streamlit as st
import back_end as be

st.title("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")

channel_Name = st.text_input('Enter Youtube channel name (Youtube API -----> Mongodb)')

if st.button("Submit"):
    a = be.first(name=channel_Name)
    st.write(a[0])
    st.json(a[1])

option = st.selectbox('Select any list of youtube channel (Mongodb -----> SQL)', be.list_of_collections())
st.write('You selected:', option)

if st.button("Transfer"):
    st.write(be.mongodb_to_sql(collection_name=option))

questions = st.selectbox('Select any list of questions to query from sql DB',
                         ("1. What are the names of all the videos and their corresponding channels?",
                          "2. Which channels have the most number of videos, and how many videos do they have?",
                          "3. What are the top 10 most viewed videos and their respective channels?",
                          "4. How many comments were made on each video, and what are their corresponding video names?",
                          "5. Which videos have the highest number of likes, and what are their corresponding channel"
                          " names?",
                          "6. What is the total number of likes and view for each video, and what are their "
                          ""
                          "corresponding video names?",
                          "7. What is the total number of views for each channel, and what are their corresponding "
                          ""
                          "channel names?",
                          "8. What are the names of all the channels that have published videos in the year 2022?",
                          "9. What are the description of all videos in each channel, and what are their corresponding "
                          "channel names?",
                          "10. Which videos have the highest number of comments, and what are their corresponding "
                          "channel names?",
                          )
                         )
st.write('You selected:', questions)

if st.button("Query"):
    st.dataframe(be.last(question=questions))
