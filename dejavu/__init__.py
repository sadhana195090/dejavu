from __future__ import print_function
from dejavu.database import get_database, Database
import dejavu.decoder as decoder
import fingerprint
import multiprocessing
import os
import traceback
import sys


class Dejavu(object):

    SONG_ID = "song_id"
    SONG_NAME = 'song_name'
    CONFIDENCE = 'confidence'
    MATCH_TIME = 'match_time'
    OFFSET = 'offset'
    OFFSET_SECS = 'offset_seconds'

    def __init__(self, config):
        super(Dejavu, self).__init__()

        self.config = config

        # initialize db
        db_cls = get_database(config.get("database_type", None))

        self.db = db_cls(**config.get("database", {}))
        self.db.setup()

        # if we should limit seconds fingerprinted,
        # None|-1 means use entire track
        self.limit = self.config.get("fingerprint_limit", None)
        if self.limit == -1:  # for JSON compatibility
            self.limit = None
        self.get_fingerprinted_songs()

    def get_fingerprinted_songs(self):
        # get songs previously indexed
        self.songs = self.db.get_songs()
        self.songhashes_set = set()  # to know which ones we've computed before
        for song in self.songs:
            song_hash = song[Database.FIELD_FILE_SHA1]
            self.songhashes_set.add(song_hash)

    def fingerprint_directory(self, path, extensions, nprocesses=None):
        # Try to use the maximum amount of processes if not given.
        try:
            nprocesses = nprocesses or multiprocessing.cpu_count()
        except NotImplementedError:
            nprocesses = 1
        else:
            nprocesses = 1 if nprocesses <= 0 else nprocesses

        pool = multiprocessing.Pool(nprocesses)

        filenames_to_fingerprint = []
        for filename, _ in decoder.find_files(path, extensions):

            # don't refingerprint already fingerprinted files
            if decoder.unique_hash(filename) in self.songhashes_set:
                print("%s already fingerprinted, continuing..." % filename)
                continue

            filenames_to_fingerprint.append(filename)

        # Prepare _fingerprint_worker input
        worker_input = zip(filenames_to_fingerprint,
                           [self.limit] * len(filenames_to_fingerprint))

        # Send off our tasks
        iterator = pool.imap_unordered(_fingerprint_worker,
                                       worker_input)

        # Loop till we have all of them
        while True:
            try:
                song_name, hashes, file_hash = iterator.next()
            except multiprocessing.TimeoutError:
                continue
            except StopIteration:
                break
            except:
                print("Failed fingerprinting")
                # Print traceback because we can't reraise it here
                traceback.print_exc(file=sys.stdout)
            else:
                sid = self.db.insert_song(song_name, file_hash)

                self.db.insert_hashes(sid, hashes)
                self.db.set_song_fingerprinted(sid)
                self.get_fingerprinted_songs()

        pool.close()
        pool.join()

    def fingerprint_file(self, filepath, song_name=None):
        songname = decoder.path_to_songname(filepath)
        song_hash = decoder.unique_hash(filepath)
        song_name = song_name or songname
        # don't refingerprint already fingerprinted files
        if song_hash in self.songhashes_set:
            print("%s already fingerprinted, continuing..." % song_name)
        else:
            song_name, hashes, file_hash = _fingerprint_worker(
                filepath,
                self.limit,
                song_name=song_name
            )
            sid = self.db.insert_song(song_name, file_hash)

            self.db.insert_hashes(sid, hashes)
            self.db.set_song_fingerprinted(sid)
            self.get_fingerprinted_songs()

    def find_matches(self, samples, Fs=fingerprint.DEFAULT_FS, i=1):
        hashes = fingerprint.fingerprint(samples, Fs=Fs)
        return self.db.return_matches(hashes,i)

    def align_matches(self, matches):
        """
            Finds hash matches that align in time with other matches and finds
            consensus about which hashes are "true" signal from the audio.

            Returns a dictionary with match information.
        """
        # align by diffs
        diff_counter = {}
        largest = [0,0,0]
        largest_count = [0,0,0]
        song_id = [-1,-1,-1]
        total_hashes={}
        for tup in matches:
            sid, diff, hash_length= tup
            total_hashes[hash_length.rsplit("_")[-1]]=int(hash_length.rsplit("_")[0])
            if diff not in diff_counter:
                diff_counter[diff] = {}
            if sid not in diff_counter[diff]:
                diff_counter[diff][sid] = 0
            diff_counter[diff][sid] += 1
            #print (str(diff_counter[diff][sid]),str(diff),str(sid))


            if diff_counter[diff][sid] > largest_count[2]:

                if diff_counter[diff][sid] > largest_count[1]:

                    if diff_counter[diff][sid] > largest_count[0]:

                        if(song_id[0]!=sid or largest[0]!=diff):

                            largest[2]=largest[1]
                            largest_count[2]=largest_count[1]
                            song_id[2]=song_id[1]
                            largest[1]=largest[0]
                            largest_count[1]=largest_count[0]
                            song_id[1]=song_id[0]
                        largest[0] = diff
                        largest_count[0] = diff_counter[diff][sid]
                        song_id[0] = sid

                    else:
                        if(song_id[1]!=sid or largest[1]!=diff):
                            largest[2]=largest[1]
                            largest_count[2]=largest_count[1]
                            song_id[2]=song_id[1]
                        largest[1]=diff
                        largest_count[1]=diff_counter[diff][sid]
                        song_id[1]=sid

                else:

                    largest[2]=diff
                    largest_count[2]=diff_counter[diff][sid]
                    song_id[2]=sid
            #print (song_id,largest_count)                
        songs1=[{},{},{}]

        if(all([s_id==-1 for s_id in song_id])):
            return songs1
        # extract idenfication
        songs_g = self.db.get_songs_by_ids(song_id)
        songs=[None,None,None]
        for x in songs_g:
            for i,x1 in enumerate(song_id):
                if x.get(Database.FIELD_SONG_ID,None)==x1:
                    songs[i]=x
        
        
        #print len(song_id)
        
        for i,s_id in enumerate(song_id):
            #print(s_id)

            songname=None
            songhash=None
            songrbtid=None
            if i!=0 and s_id==song_id[i-1]:
                songname=songs1[i-1].get(Dejavu.SONG_NAME,None)
                songhash=songs1[i-1].get(Database.FIELD_FILE_SHA1,None)
                songrbtid=songs1[i-1].get(Database.FIELD_RBT_ID,None)
            else:
                try:
                    song=songs[i]
                    
                    if song:
                        # TODO: Clarify what `get_song_by_id` should return.
                        songname = song.get(Dejavu.SONG_NAME, None)
                        songhash=song.get(Database.FIELD_FILE_SHA1, None)
                        songrbtid=song.get(Database.FIELD_RBT_ID,None)
                    #else:
                        #return None
            
                except Exception as e:
                    print("error "+str(e))
                    break
            

            # return match info
            nseconds = round(float(largest[i]) / fingerprint.DEFAULT_FS *
                             fingerprint.DEFAULT_WINDOW_SIZE *
                             fingerprint.DEFAULT_OVERLAP_RATIO, 5)
            song1 = {
                Dejavu.SONG_ID : s_id,
                Dejavu.SONG_NAME : songname,
                Dejavu.CONFIDENCE : largest_count[i],
                "total_hashes"  :   sum(total_hashes.values()),
                Dejavu.OFFSET : int(largest[i]),
                Dejavu.OFFSET_SECS : nseconds,
                Database.FIELD_RBT_ID	: songrbtid,
                Database.FIELD_FILE_SHA1 : songhash,}
            song1["threshold"]=song1[Dejavu.CONFIDENCE]*1.0/song1["total_hashes"]

            songs1[i]=song1
        return songs1

    def recognize(self, recognizer, *options, **kwoptions):
        r = recognizer(self)
        return r.recognize(*options, **kwoptions)


def _fingerprint_worker(filename, limit=None, song_name=None):
    # Pool.imap sends arguments as tuples so we have to unpack
    # them ourself.
    try:
        filename, limit = filename
    except ValueError:
        pass

    songname, extension = os.path.splitext(os.path.basename(filename))
    song_name = song_name or songname
    channels, Fs, file_hash = decoder.read(filename, limit)
    result = set()
    channel_amount = len(channels)

    for channeln, channel in enumerate(channels):
        # TODO: Remove prints or change them into optional logging.
        print("Fingerprinting channel %d/%d for %s" % (channeln + 1,
                                                       channel_amount,
                                                       filename))
        hashes = fingerprint.fingerprint(channel, Fs=Fs)
        print("Finished channel %d/%d for %s" % (channeln + 1, channel_amount,
                                                 filename))
        result |= set(hashes)

    return song_name, result, file_hash


def chunkify(lst, n):
    """
    Splits a list into roughly n equal parts.
    http://stackoverflow.com/questions/2130016/splitting-a-list-of-arbitrary-size-into-only-roughly-n-equal-parts
    """
    return [lst[i::n] for i in xrange(n)]
