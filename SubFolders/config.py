### This file stores all of our configuration variables
### These variables are things like the endpoint url, as well as any paths where we are saving files
class VARIABLES():
    def __init__(self):
        #end_point is the url to the folder directory
        self.end_point = 'https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip'
        
        #save_path is the file path where you wish to save the zipped folder that the s3 endpoint will give you. Note that the last piece 'Files.zip' is what the zipped folder will be called and is necessary
        self.save_path = 'C:/Users/daveb/OneDrive/Documents/Adult Life/Jobs/Guild_Education/Files.zip'
        
        #zip_path is the path to where the zip files can be found
        self.zip_path = 'C:/Users/daveb/OneDrive/Documents/Adult Life/Jobs/Guild_Education/'
        
        #csv_extract_path is where you want the extracted csv files from the zipped folder to go. The last piece is the folder the files will go into
        self.csv_extract_path = 'C:/Users/daveb/OneDrive/Documents/Adult Life/Jobs/Guild_Education/CSVFiles'
        
        #collist is the list of columns that you want to de-stringify and turn into a table for the data model
        self.collist = ['genres', 'production_companies']
        
        #sql_file_path is the file path where you create the sqlite database
        self.sql_file_path = 'C:/Users/daveb/OneDrive/Documents/Adult Life/Jobs/Guild_Education/sqlite/db/GuildEducationProject.db'