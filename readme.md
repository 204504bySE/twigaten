# TwiGaTen
Twitter上の類似画像を一覧表示するやつ。
<https://twigaten.204504byse.info/>

# 各フォルダの中身
* Crawl
  * ツイートと画像を収集する奴
* CrawlParent
  * Crawlにアカウントを割り当てたり死活監視するやつ
  * 複数プロセスの機能は残骸しか残ってない
* Lock
  * Crawlを複数起動するときに使ってたけど今はただの残骸
* Hash
  * 画像のハッシュ値の類似ペアを生成して類似画像の範囲を広げる奴
* Tool
  * ツイ消しされた画像を消したりする奴(といろいろな残骸)
* Web
  * HTMLを吐く奴
* DctHash
  * 公開当初にリサイズをgdiplusに丸投げしたゆえの負債

# 使い方のようなもの
  * twiten.sql.txt の中身を丸ごとMySQLにぶっ込んでDBを作る
  * twiten.ini.sample を適宜書き換えて、twiten.iniにリネームして各exeと同じディレクトリに置く
  * CrawlParentを実行してtwidownが起動するのを確認
  * 画像をキャッシュするHDD/SSDが溢れないようにToolを定期実行させる
  * 暇なときにHashを動かして類似画像の範囲を広げる
  * ↑はsystemdサービスを作るといいよ

