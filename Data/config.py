from configparser import ConfigParser
import os

def config(filename = 'database.ini', section = 'postgresql'):
    # Used for retreive user sensitive information to database

    #Create parser
    parser = ConfigParser()

    #Get the real path of this file
    abspath = os.path.dirname(os.path.realpath(__file__)) + '\\'

    # Read the configfile 
    parser.read(abspath + filename)

    # Get the section, default postgresql, of the filename.
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} was not found in {filename}')

    return db

if __name__ == '__main__':
    config()
