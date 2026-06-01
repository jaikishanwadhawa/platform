import requests, time, os, json, re
import configparser

## Note: rate-limit (429 code) handling is only partially implemented in this script

cfg = configparser.ConfigParser()
cfg.read("config.ini")
configSection = cfg.get("DEFAULT", "CONFIG")
protocol = cfg.get(configSection, "PROTOCOL")
hostPort = cfg.get(configSection, "HOSTPORT")
JWT = cfg.get(configSection, "JWT")
urlPrefix = cfg.get(configSection, "URLPREFIX")
inputFolder = cfg.get("DEFAULT", "INPUTFOLDER")
inputFname = cfg.get("DEFAULT", "INPUTFILE")
outputFolder = cfg.get("DEFAULT", "OUTPUTFOLDER")

model = "VoiceGain-omega"
#model = None
#model = "whisper:medium"

print("model: {}".format(model))

if not os.path.exists(outputFolder):
    os.makedirs(outputFolder)

host = f"{protocol}://{hostPort}/{urlPrefix}"

print("host: {}".format(host))

asr_body = {
    "sessions": [
        {
            "asyncMode": "OFF-LINE",
            #"initialPrompt": "I am talking to a customer support agent.",
            "audioChannelSelector" : "two-channel",
            #"initialPrompt": "I am a support Agent talking to a customer.",
            "poll": {
                # will delete the session after 1 minute
                #"afterlife": 60000
                "persist" : 599999
            },
            "content": {
                "incremental": ["progress"],
                "full" : ["transcript", "words"]
            }
        }
    ],
    "audio":{
        "source": {
            "dataStore": {
                "uuid": "to be filled later"
            }
        }
    },
    "settings": {
        "asr": {
            "acousticModelNonRealTime": model,
            "confidenceThreshold": 0.01,
            "sensitivity": 0.5,
            "languages": ["es-mx", "en-us"]
        },
        "dtmf": {
            "recognize": False,
            "redact": False
        },
        "formatters": [
            {
                "type": "digits"
            },
            {
                "type": "enhanced",
                "parameters": {
                    "CC": True,
                    "CVV": True,
                    "SSN": True
                }
            },
            {
                "type": "spelling",
                "parameters": {"lang": "es"}
            },
            {
                "type": "redact",
                "parameters": {
                    "CC": "partial:4",
                    "CVV": "full:2",
                    "SSN": "partial:4"
                }
            }
        ]
    }
}

#### all settings above this line ####

audio_type = "audio/wav"

output_path = "{}/{}".format(outputFolder, time.strftime("%Y-%m-%d_%H-%M-%S"))
if not os.path.exists(output_path):
    os.makedirs(output_path)

data_url = "{}/data/file".format(host)

headers = {"Authorization":JWT}

def process_one_file(audio_fname):
    ## steps:
    ## 1. upload audio
    ## 2. start offline transcription session (two-channel)
    ## 3. keep polling untill we are done
    ## 4. retrieve transcript as text and as json-mc (multi-channel)

    path, fname = os.path.split(audio_fname)

    print("Processing {}/{}".format(path,fname), flush=True)

    data_body = {
        "name" : re.sub("[^A-Za-z0-9]+", "-", fname),
        "description" : audio_fname,
        "contentType" : audio_type,
        "tags" : ["test"]
    }

    multipart_form_data = {
        'file': (audio_fname, open(audio_fname, 'rb'), audio_type),
        'objectdata': (None, json.dumps(data_body), "application/json")
    }
    print("uploading audio data {} ...".format(audio_fname), flush=True)

    data_response = None
    data_response_raw = None
    try:
        data_response_raw = requests.post(data_url, files=multipart_form_data, headers=headers)
        code = data_response_raw.status_code
        print("   response code: {}".format(code))

        if(code != 200 and code != 429):
            print("unexpected response code")
            print(data_response_raw.text)
            exit()

        resp_headers = data_response_raw.headers
        print("response headers: {}".format(resp_headers))

        ## note: ideally we should add rate-limit response handling also in the asr request
        if(code == 429):
            retry_after = resp_headers.get("Retry-After")
            if(retry_after is None):
                print("rate limit exceeded but response missing Retry-After")
                exit()
            return int(retry_after)

        data_response = data_response_raw.json()
    except Exception as e:
        print(str(data_response_raw))
        exit()

    print("data response: {}".format(data_response), flush=True)

    if data_response.get("status") is not None and data_response.get("status") == "BAD_REQUEST":
        print("error uploading file {}".format(audio_fname), flush=True)
        exit()

    object_id = data_response["objectId"]
    print("objectId: {}".format(object_id), flush=True)

    ## set the audio id in the asr request
    if not os.path.exists(output_path):
        os.mkdir(output_path)

    asr_body["audio"]["source"]["dataStore"]["uuid"] = object_id

    printTranscribeQueueStatus()

    print("making asr request ...", flush=True)
    asr_response_raw = requests.post("{}/asr/transcribe/async".format(host), json=asr_body, headers=headers)
    start_time = time.time()
    if(asr_response_raw.status_code != 200):
        print("unexpected response code {} for asr request".format(asr_response_raw.status_code), flush=True)
        print(asr_response_raw.text , flush=True)
        exit()

    asr_response = asr_response_raw.json()
    session_id = asr_response["sessions"][0]["sessionId"]
    polling_url = asr_response["sessions"][0]["poll"]["url"]

    index = 0
    ## poll untill we have final result
    while True:
        if(index == 0):
            #first
            print("no wait for first poll request")
        elif(index<5):
            time.sleep(0.3)
        else:
            time.sleep(4.9)

        elapsed_time = time.time() - start_time
        print("Time taken just before poll request:", elapsed_time, "seconds")
        poll_response_raw = requests.get(polling_url+"?full=false", headers=headers)
        elapsed_time = time.time() - start_time
        print("Time taken just after poll request:", elapsed_time, "seconds")

        code = poll_response_raw.status_code
        print("   response code: {}".format(code))

        if(code != 200 and code != 429):
            print("unexpected response code")
            exit()

        if(code == 429):
            retry_after = resp_headers.get("Retry-After")
            if(retry_after is None):
                print("rate limit exceeded but response missing Retry-After")
                exit()
            time.sleep(retry_after)
            continue

        poll_response = poll_response_raw.json()
        phase = poll_response["progress"]["phase"]
        is_final = poll_response["result"]["final"]
        print("Phase: {} Final: {}".format(phase, is_final), flush=True)

        # write each poll_response to JSON (combined poll/progress log)
        poll_response_path = os.path.join(output_path, "{}-poll-{:03d}.json".format(session_id, index))
        with open(poll_response_path, 'w', encoding='utf-8') as outfile:
            json.dump(poll_response, outfile, indent=4, ensure_ascii=False)
        print("Phase: {} Final: {} -> Save poll result to {}".format(phase, is_final, poll_response_path), flush=True)

        index += 1
        if is_final:
            break

    # write full response
    if(True):
        poll_response_raw = requests.get(polling_url+"?full=true", headers=headers)
        print(poll_response_raw.headers['Content-Type'])
        poll_response = poll_response_raw.json()
        phase = poll_response["progress"]["phase"]
        print("Phase: {} Final: {}".format(phase, is_final), flush=True)
        poll_response_path = os.path.join(output_path, "{}--{}.json".format(session_id, index))
        with open(poll_response_path, 'w', encoding='utf-8') as outfile:
            json.dump(poll_response, outfile, indent=4, ensure_ascii=False)
        print("Save final result to {}".format(poll_response_path), flush=True)

    #get result as text file

    txt_url = "{}/asr/transcribe/{}/transcript?format=text".format(host, session_id)
    print("Retrieving transcript using url: {}".format(txt_url), flush=True)
    txt_response = requests.get(txt_url, headers=headers)
    txt_response.encoding = txt_response.apparent_encoding ## << needed to get the encoding correct
    transcript_text_path = os.path.join(output_path, "{}.txt".format(fname))
    with open(transcript_text_path, 'w', encoding='utf-8') as file_object:
        file_object.write(txt_response.text)
    print("Save final transcript text to {}".format(transcript_text_path))
    print("", flush=True)

    #get result as json-mc file (multi-channel transcript)

    txt_url = "{}/asr/transcribe/{}/transcript?format=json-mc".format(host, session_id)
    print("Retrieving transcript using url: {}".format(txt_url), flush=True)
    txt_response = requests.get(txt_url, headers=headers)
    txt_response.encoding = txt_response.apparent_encoding ## << needed to get the encoding correct
    transcript_text_path = os.path.join(output_path, "{}.json".format(fname))
    with open(transcript_text_path, 'w', encoding='utf-8') as file_object:
        # Parse and pretty-format the JSON response
        try:
            json_data = txt_response.json()
            file_object.write(json.dumps(json_data, indent=4, ensure_ascii=False))
        except json.JSONDecodeError:
            # If response is not valid JSON, write as-is
            file_object.write(txt_response.text)
    print("Save final transcript json-mc to {}".format(transcript_text_path))
    print("", flush=True)

    printTranscribeQueueStatus()

    return -1

def printTranscribeQueueStatus():
    print("making asr queue request ...", flush=True)
    asr_response_raw = requests.get("{}/asr/transcribe/status/queue".format(host), headers=headers)
    if(asr_response_raw.status_code != 200):
        print("unexpected response code {} for asr request".format(asr_response_raw.status_code), flush=True)
    else:
        asr_response = asr_response_raw.json()
        pretty_asr_response = json.dumps(asr_response, indent=4)  # Pretty-printing here
        print("asr queue status:\n{}".format(pretty_asr_response), flush=True)

## MAIN ##

print("START", flush=True)

name = os.path.join(inputFolder, inputFname)
print("name: {}".format(name), flush=True)

retry_after = process_one_file(name)
while(retry_after >=0 ):
    print("rate-limit hit - need to wait {} seconds".format(retry_after), flush=True)
    time.sleep(retry_after)
    print("will retry now", flush=True)
    retry_after = process_one_file(name)


print("THE END", flush=True)
