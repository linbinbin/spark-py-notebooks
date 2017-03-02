# Spark Python Notebooks  

これは、基本的なものから高度なものまで、さまざまなApache Sparkの概念をPython言語を使用して訓練するための、IPythonノートブック/ Jupyterノートブックのコレクションです。

もしPythonがあなたの言語ではなく、それがRならば、代わりにApache Spark（SparkR）のRノートブックを見てみてください。 さらに、いくつかの基本的なデータサイエンスエンジニアリングに興味がある場合は、これらの一連のチュートリアルが興味深いかもしれません。 ここでは、PythonとRを使用して、さまざまな概念とアプリケーションについて説明します。


## Instructions  説明書

これらのノートブックを使用する良い方法は、最初にrepoをクローンし、pySparkモードで自分のIPythonノートブック/ Jupyterを起動することです。 たとえば、IPythonに割り当てられたノードあたり最大6Gbのローカルホストで実行されるスタンドアロンのSparkインストールがあるとします。

    MASTER="spark://127.0.0.1:7077" SPARK_EXECUTOR_MEMORY="6G" IPYTHON_OPTS="notebook --pylab inline" ~/spark-1.5.0-bin-hadoop2.6/bin/pyspark

pysparkコマンドのパスは、特定のインストールによって異なります。 したがって、要件として、IPythonノートブックサーバを起動するマシンにSparkをインストールする必要があります。

その他のスパークオプションについてはこちらをご覧ください。 一般に、IPython / pySparkを呼び出すときにspark.executor.memoryという形式で記述されたオプションをSPARK_EXECUTOR_MEMORYとして渡すルールが働きます。

 
## Datasets  

KDDカップ1999のデータセットを使用します。この競技の結果はここにあります。
[here](http://cseweb.ucsd.edu/~elkan/clresults.html)
[KDD Cup 1999](http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html)

競合タスクは、侵入または攻撃と呼ばれる「悪い」接続と正常な接続を区別できるネットワーク侵入検出器を構築することでした。
このデータベースには、軍事ネットワーク環境でシミュレートされたさまざまな侵入を含む、監査対象の標準データセットが含まれています。

## References

これらおよびその他のSpark関連トピックのリファレンスブックは次のとおりです。 :  
- *Holden Karau、Andy Konwinski、Patrick Wendell、Matei ZahariaによるSparkの学習。


## Notebooks  

次のノートブックは個別に進めることができますが、順番に従うとそこには「ストーリー」があります。同じデータセットを使用することによって、関連する一連のタスクを解決しようとします。

 
### [RDDの作成](https://github.com/miyamotok0105/spark-py-notebooks/blob/master/nb1-rdd-creation/nb1-rdd-creation.ipynb)  

ファイルの読み込みと並列化について
  
### [RDDの基礎](https://github.com/miyamotok0105/spark-py-notebooks/blob/master/nb2-rdd-basics/nb2-rdd-basics.ipynb)

map, filter, collectについて
  
### [RDDのサンプリング](https://github.com/miyamotok0105/spark-py-notebooks/blob/master/nb3-rdd-sampling/nb3-rdd-sampling.ipynb)  

RDDサンプリング方法を説明。
  
### [RDDセット操作](https://github.com/miyamotok0105/spark-py-notebooks/blob/master/nb4-rdd-set/nb4-rdd-set.ipynb)    

いくつかのRDD擬似セット操作の簡単な紹介。

### [RDD上のデータ集約](https://github.com/miyamotok0105/spark-py-notebooks/blob/master/nb5-rdd-aggregations/nb5-rdd-aggregations.ipynb)  

RDDアクション `reduce`, `fold`, `aggregate`について 。

### [キーと値のペアRDDの操作](https://github.com/miyamotok0105/spark-py-notebooks/blob/master/nb6-rdd-key-value/nb6-rdd-key-value.ipynb)    

データを集約して探索するためのキーと値のペアの扱い方。
  
### [MLlib：基本統計と探索的データ解析](https://github.com/miyamotok0105/spark-py-notebooks/blob/master/nb7-mllib-statistics/nb7-mllib-statistics.ipynb)    

ローカルベクトルタイプ、Exploratory Data Analysisおよびモデル選択のためのMLlibの基本統計を紹介するノートブック。
  
### [MLlib：ロジスティック回帰](https://github.com/miyamotok0105/spark-py-notebooks/blob/master/nb8-mllib-logit/nb8-mllib-logit.ipynb)     

MLlibにおけるネットワーク攻撃のラベル付けされたポイントとロジスティック回帰の分類。相関行列と仮説検定を用いたモデル選択手法の応用。

### [MLlib：デシジョンツリー](https://github.com/miyamotok0105/spark-py-notebooks/blob/master/nb9-mllib-trees/nb9-mllib-trees.ipynb)  

ツリーベースの方法の使用、およびモデルと機能の選択の説明に役立つ方法。

### [Spark SQL：データ分析のための構造化処理](https://github.com/miyamotok0105/spark-py-notebooks/blob/master/nb10-sql-dataframes/nb10-sql-dataframes.ipynb)  

このノートブックでは、ネットワークインタラクションのデータセットに対してスキーマが推論されます。それに基づいて、SparkのSQL DataFrame抽象化を使用して、より構造化された探索的データ分析を実行します。 


## アプリケーション  

基礎の先へ。 Sparkなどのテクノロジーを使用した実世界のアプリケーションに近い

### [Olssen: On-line Spectral Search ENgine for proteomics](https://github.com/jadianes/olssen)  

Same tech stack this time with an AngularJS client app.  

### [An on-line movie recommendation web service](https://github.com/jadianes/spark-movie-lens)  

This tutorial can be used independently to build a movie recommender model based on the MovieLens dataset. Most of the code in the first part, about how to use ALS with the public MovieLens dataset, comes from my solution to one of the exercises proposed in the [CS100.1x Introduction to Big Data with Apache Spark by Anthony D. Joseph on edX](https://www.edx.org/course/introduction-big-data-apache-spark-uc-berkeleyx-cs100-1x), that is also [**publicly available since 2014 at Spark Summit**](https://databricks-training.s3.amazonaws.com/movie-recommendation-with-mllib.html). 

There I've added with minor modifications to use a larger dataset and also code about how to store and reload the model for later use. On top of that we build a Flask web service so the recommender can be use to provide movie recommendations on-line.  

### [KDD Cup 1999](https://github.com/jadianes/kdd-cup-99-spark)  

My try using Spark with this classic dataset and Knowledge Discovery competition.  

## Contributing

Contributions are welcome!  For bug reports or requests please [submit an issue](https://github.com/jadianes/spark-py-notebooks/issues).

## Contact  

Feel free to contact me to discuss any issues, questions, or comments.

* Twitter: [@ja_dianes](https://twitter.com/ja_dianes)
* GitHub: [jadianes](https://github.com/jadianes)
* LinkedIn: [jadianes](https://www.linkedin.com/in/jadianes)
* Website: [jadianes.me](http://jadianes.me)

## License

This repository contains a variety of content; some developed by Jose A. Dianes, and some from third-parties.  The third-party content is distributed under the license provided by those parties.

The content developed by Jose A. Dianes is distributed under the following license:

    Copyright 2016 Jose A Dianes

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
