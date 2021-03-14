using System;
using System.Collections.Generic;
using System.Text;
using MessagePack;

namespace Twigaten.Lib
{
    /// <summary>
    /// DctHashが受け付けるobject
    /// </summary>
    [MessagePackObject]
    public class PictHashRequest
    {
        /// <summary>
        /// media_idなどを入れて識別できる
        /// </summary>
        [Key(0)]
        public long UniqueId { get; set; }
        /// <summary>
        /// trueなら画像を正方形に切り抜く
        /// </summary>
        [Key(1)]
        public bool Crop { get; set; }
        /// <summary>
        /// 画像ファイルそのもの
        /// </summary>
        [Key(2)]
        public byte[] MediaFile { get; set; }
    }

    /// <summary>
    /// DctHashが返すobject
    /// </summary>
    [MessagePackObject]
    public class PictHashResult
    {
        /// <summary>
        /// media_idなどを入れて識別できる
        /// </summary>
        [Key(0)]
        public long UniqueId { get; set; }
        /// <summary>
        /// ハッシュ値 失敗したらnull
        /// </summary>
        [Key(1)]
        public long? DctHash { get; set; }
    }
}
