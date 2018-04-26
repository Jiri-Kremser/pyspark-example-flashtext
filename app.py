import os
from flask import Flask
from flask import request
from pyspark.sql import SparkSession


app = Flask(__name__)

def process_keywords(sentence, keyword):
    n = 50
    spark = SparkSession.builder.appName("Keywords").getOrCreate()

    # udf
    def f(s):
        from flashtext import KeywordProcessor
        keyword_processor = KeywordProcessor(case_sensitive=False)
        keyword_processor.add_keyword(keyword)
        return keyword_processor.extract_keywords(sentence, span_info=True)

    # create n sentences
    sentences = [sentence] * n
    # spark.sparkContext.parallelize(sentences).map(f).toDF().show(10)
    return spark.sparkContext.parallelize(sentences).map(f).take(1)[0]


@app.route("/")
def root():
    sentence = request.args.get('text', 'The quick brown fox jumps over the lazy dog')
    keyword = request.args.get('keyword', 'fox')
    result = process_keywords(sentence, keyword)
    print(result)
    if result == []:
        return 'not found'
    else:
        return result[0][0] + ' start at: ' + str(result[0][1]) + ' end at: ' + str(result[0][2])


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)