from elasticsearch import Elasticsearch
from flask import Flask, render_template, request, url_for, request, redirect
import jsonify
import json
import re
app = Flask(__name__)

genre_list = {
    "hip hop",
    "pop",
    "metal",
    "indie",
    "rap",
    "funk",
    "r&b",
    "soul",
    "k-pop",
    "else",
    "rock",
    "dance",
    "electronic",
    "latin",
    "trap",
    "country",
    "house",
    "reggaeton",
    "boy band",
    "bolero",
    "reggae",
    "jazz",
    "opm"
}

country_list = {
    "global",
    "singapore",
    "chile",
    "uk",
    "taiwan",
    "malaysia",
    "usa",
    "denmark",
    "belgium",
    "brazil",
    "canada",
    "germany",
    "argentina",
    "spain",
    "sweden",
    "australia",
    "france",
    "poland",
    "austria",
    "finland",
    "new zealand",
    "costa rica",
    "ireland",
    "switzerland",
    "philippines",
    "norway",
    "netherlands",
    "colombia",
    "ecuador",
    "peru",
    "portugal",
    "italy",
    "mexico",
    "indonesia",
    "turkey"
}


@app.route("/getEs/<query>")
def get_es(query):
    es = Elasticsearch(['http://localhost:9200'], request_timeout=3600)
    # body = {
    #     "size": 20,
    #     "query": {
    #         "dis_max": {
    #             "queries": [
    #                 {"match": {"query": query}}
    #             ],
    #             "tie_breaker": 0.3
    #         }
    #     }
    # }

    str = query
    year_boolean = False
    if re.search("\d", str):
        year = re.findall(r"\d+", str)[0]
        year = int(year)
        year_boolean = True

    genre_boolean = False
    for term in genre_list:
        if term in query:
            genre = term

            genre_boolean = True

    if year_boolean:
        date = "% s" % year
        query = query.replace(date, "")
        if year >= 1900 and year <= 2022:
            if genre_boolean:
                query = {
                    "bool": {
                        "must":     [
                            {"term": {"genre": genre}},
                            {"term": {"date": date}}
                        ],
                        "should": [
                            {
                                "function_score": {
                                    "query": {
                                        "multi_match": {
                                            "query": query,
                                            "fields": [
                                                "title",
                                                "artist"
                                            ]
                                        }
                                    },
                                    "field_value_factor":{
                                        "field": "popularity",
                                        "factor": 2,
                                        "modifier": "log1p"
                                    }
                                }
                            }
                        ]
                    }
                }
            else:

                query = {
                    "bool": {
                        "must":     [{"match": {"date": date}}],
                        "should": [
                            {
                                "function_score": {
                                    "query": {
                                        "multi_match": {
                                            "query": query,
                                            "fields": [
                                                "title",
                                                "artist"
                                            ]
                                        }
                                    },
                                    "field_value_factor":{
                                        "field": "popularity",
                                        "factor": 2,
                                        "modifier": "log1p"
                                    }
                                }
                            }
                        ]
                    }
                }
        else:
            query = {
                "bool": {
                    "should": [
                        {
                            "function_score": {
                                "query": {
                                    "multi_match": {
                                        "query": query,
                                        "fields": [
                                            "title",
                                            "artist"
                                        ]
                                    }
                                },
                                "field_value_factor":{
                                    "field": "popularity",
                                    "factor": 2,
                                    "modifier": "log1p"
                                }
                            }
                        }
                    ]
                }
            }
    elif genre_boolean:
        query = {
            "bool": {
                "must":     [
                    {"term": {"genre": genre}}
                ],
                "should": [
                    {
                        "function_score": {
                            "query": {
                                "multi_match": {
                                    "query": query,
                                    "fields": [
                                        "title",
                                        "artist"
                                    ]
                                }
                            },
                            "field_value_factor":{
                                "field": "popularity",
                                "factor": 2,
                                "modifier": "log1p"
                            }
                        }
                    }
                ]
            }
        }
    else:
        query = {
            "bool": {
                "should": [
                    {
                        "function_score": {
                            "query": {
                                "multi_match": {
                                    "query": query,
                                    "fields": [
                                        "title",
                                        "artist"
                                    ]
                                }
                            },
                            "field_value_factor":{
                                "field": "popularity",
                                "factor": 2,
                                "modifier": "log1p"
                            }
                        }
                    }
                ]
            }
        }

    # if genre_boolean:
    #     if year >= 1900 and year <= 2022:
    # date = "% s" % year
    # query = {
    #     "bool": {
    #         "must":     [
    #             {"term": {"genre": genre}},
    #             {"term": {"date": date}}
    #         ],
    #         "should": [
    #             {"match": {"title": {"query": query, "boost": 10}}},
    #             {"match": {"artist": query}},
    #             # {"match": {"date": "2017"}}
    #         ]
    #     }
    # }
    #     else:
    #         query = {
    #             "bool": {
    #                 "must":     [{"match": {"genre": genre}}],
    #                 "should": [
    #                     {"match": {"title": {"query": query, "boost": 10}}},
    #                     {"match": {"artist": query}}
    #                     # {"match": {"date": "2017"}}
    #                 ]
    #             }
    #         }
    # else:

    # query = {
    #     "bool": {
    #         "should": [
    #             {"match": {"title": {"query": query}}},
    #             {"match": {"artist": query}}
    #             # {"match": {"date": "2017"}}
    #         ]
    #     }
    # }

    data = es.search(index='spotify_dataset', query=query, size=20)
    address_data = data['hits']['hits']
    print(data['hits'])
    address_list = []
    for item in address_data:
        address_list.append(
            {'title': item['_source']['title'], 'uri': item['_source']['uri'], 'artist': item['_source']['artist'], 'date': item['_source']['date'], 'genre': item['_source']['genre'], 'popularity': item['_source']['popularity']})
    new_data = json.dumps(address_list)
    return address_list


@app.route('/', methods=['POST', 'GET'])
def index():

    if request.method == 'GET':
        return render_template('index.html')
    else:
        query = request.form['content']
        tasks = get_es(query)

        return render_template('index.html', tasks=tasks)


if __name__ == '__main__':
    app.run(host='localhost', port=5001, debug=True)
