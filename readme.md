# TwiGaTen
Twitter上の類似画像を一覧表示するやつ。
<https://twigaten.204504byse.info/>

…の .NET Core 版

# 各フォルダの中身
* twidown
  * ツイートと画像を収集する奴
* twidownparent
  * twidownを複数起動してアカウントを割り当てる奴
* twihash
  * 画像のハッシュ値の類似ペアを生成して類似画像の範囲を広げる奴
* twitool
  * ツイ消しされた画像を消したりする奴(といろいろな残骸)
* twiview
  * HTMLを吐く奴(twigatenから拾ってね)

# 使い方のようなもの
  * twiten.sql.txt の中身を丸ごとMySQLにぶっ込んでDBを作る
  * twiten.ini.sample を適宜書き換えて、twiten.iniにリネームして各exeと同じディレクトリに置く
  * twiviewはtwigatenのやつを使う
  * twidownparentを実行してtwidownが起動するのを確認
  * 画像をキャッシュするHDD/SSDが溢れないようにtwitoolを定期実行させる
  * 暇なときにtwihashを動かして類似画像の範囲を広げる

# ライセンス
MITライセンスにした(｀・ω・´)
