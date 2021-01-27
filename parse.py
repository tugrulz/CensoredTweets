import os
import json
from tj_parser import Parser
from create_views import create_views
import shutil
import argparse
import pandas as pd
import zipfile

'''
TODO: HANDLE BIG FILES BY WRITING TO NEW FOLDER
'''
def main():
    input_folder = './input/'
    output_folder = './output/'
    write_folder = ''
    TIMELINE = False
    HYDRATED = False
    NO_VIEWS = False

    parser = argparse.ArgumentParser(
        description="Parse seqeunce of JSON formated activities.", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-i", "--input", dest="input_folder",
                        default=input_folder,
                        help="Name of the folder to read from, defaults to input")
    parser.add_argument("-o", "--output", dest="output_folder",
                        default=output_folder,
                        help="Name of the file to write to, defaults to output")

    parser.add_argument("-w", "--write_folder", dest="write_folder",
                        default=write_folder,
                        help="Name of the file to put the tables, defaults to created_tables_firstfile")

    parser.add_argument('--timeline', dest='TIMELINE', action='store_true', default=TIMELINE)
    parser.add_argument('--hydrated', dest='HYDRATED', action='store_true', default=HYDRATED)
    parser.add_argument('--no_views', dest='CREATE_VIEWS', action='store_true', default=NO_VIEWS)


    options = parser.parse_args()
    input_folder = "./" + options.input_folder + "/"
    output_folder = "./" + options.output_folder + "/"
    write_folder = options.write_folder + "/"
    TIMELINE = options.TIMELINE
    #TIMELINE = False # Timeline will be inferred instead of user input
    HYDRATED = options.HYDRATED

    print((input_folder, output_folder, write_folder, TIMELINE, HYDRATED))

    return input_folder, output_folder, write_folder, TIMELINE, HYDRATED, NO_VIEWS

def check_if_timeline(json_obj):
    if ('in_reply_to_status_id' in json_obj and 'reply_count' in json_obj):
        timeline = False
        return timeline
    elif ('in_reply_to_status_id' in json_obj and 'reply_count' not in json_obj):
        timeline = True
        return timeline
    else:
        print("There is a logical error. The parser might be deprecated. Contact the developer.")
        exit()

def process(input_folder, output_folder, write_folder, TIMELINE, HYDRATED):
    if (not os.path.exists(output_folder)):
        os.mkdir(output_folder)

    files = os.listdir(input_folder)
    files = [file for file in files if('.json' in file)]

    if(write_folder == ''):
        write_folder = "created_tables_" + files[0].split('.json')[0] + "/"
    # else:
    #     write_folder += "/"

    output_path = output_folder + write_folder

    print("Writing to %s" % output_path)

    if(not os.path.exists(output_path)):
        os.mkdir(output_path)
    else:
        shutil.rmtree(output_path)
        os.mkdir(output_path)

    handler = Parser(output_path, TIMELINE, HYDRATED)
    handler.create_writers()

    for i, file in enumerate(sorted(files)):
        print("Processing %s | %d / %d " % (file, i, len(files)))
        file = input_folder + file

        source = open(file)

        for index, line in enumerate(source):
            try:
                json_obj = json.loads(line)
            except json.decoder.JSONDecodeError:
                print("Error on %d" % index)
                continue

            #if (index == 0):
            #    timeline = check_if_timeline(json_obj)
            #    handler.no_reply_quote = timeline
            handler.no_reply_quote = TIMELINE

            handler.parse_obj(json_obj)

        source.close()

    handler.get_stats()
    handler.close_writers()

    return output_path

def postprocess(folder):
    print("Postprocessing")

    files = os.listdir(folder)

    # keep last from user, tweet metadata, sensitive, user_timezone according to id
    to_delete_by_id = ['tweet_metadata.csv', 'twitter_user.csv', 'user_profile.csv']

    rest = [file for file in files if file not in to_delete_by_id]

    files = os.listdir(folder)
    for dele in to_delete_by_id:
        if dele in files:
            df = pd.read_csv(folder + dele, keep_default_na=False)#, lineterminator = '\n')
            df = df.sort_values('access', ascending=True).drop_duplicates('id', keep = 'last')
            df.to_csv(folder + dele, index=False)

    for dele in rest:
        if dele in files:
            df = pd.read_csv(folder + dele, keep_default_na=False)
            df = df.drop_duplicates(keep='last')

            if(len(df) == 0): # if empty
                os.remove(folder + dele)
            else:
                df.to_csv(folder + dele, index=False)

if __name__ == '__main__':
    input_folder, output_folder, write_folder, TIMELINE, HYDRATED, NO_VIEWS = main()
    output_path = process(input_folder, output_folder, write_folder, TIMELINE, HYDRATED)
    postprocess(output_path)
    if(not NO_VIEWS):
        create_views(output_path)