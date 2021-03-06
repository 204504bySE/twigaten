using System;
using System.Runtime.InteropServices;
using System.IO;
using System.Drawing;
using System.Drawing.Imaging;
using System.Linq;
using System.Numerics;
using System.Buffers;
using SixLabors.ImageSharp.PixelFormats;
using SixLabors.ImageSharp.Advanced;

namespace Twigaten.DctHash
{
    static class PictHash
    {
        //static readonly float[,] dct32; //[1,1]-[8,8]だけ
        static readonly float[] cos32_1_8;
        static readonly int VectorCount;    //Vector<float>の要素数
        static readonly int xCount; //dcthash用
        static readonly Vector<float>[] cos32Vector_1_8; //横方向にVectorCountずつ取った奴
        static PictHash()
        {
            //DCT係数テーブル 対称行列だからxyは気にするな
            //[1,1]-[8,8]だけを切り抜いて格納
            /* ということは使う部分は全部 1/64 なので完全に用なし
            dct32 = new float[8, 8];
            for (int u = 1; u < 9; u++)
                for (int v = 1; v < 9; v++)
                {
                    if (u * v == 0) { dct32[u - 1, v - 1] = (float)Math.Sqrt(0.5) / 64; }
                    else { dct32[u - 1, v - 1] = 1F / 64F; }
                }
            //dct32[0, 0] = 0.5F / 64F; 原点をずらしたから用なし
            */
            //Cosテーブル[行 << 5 | 列]
            //1~8行を切り抜いておく
            cos32_1_8 = new float[256];
            for (int y = 0; y < 8; y++)
                for (int x = 0; x < 32; x++)
                {
                    cos32_1_8[y << 5 | x] = (float)(Math.Cos((2 * x + 1) * (y + 1) * Math.PI / 64));
                }

            VectorCount = Math.Min(32, Vector<float>.Count);
            xCount = 32 / VectorCount;

            //こっちも1~8行だけ使う
            cos32Vector_1_8 = new Vector<float>[256 / VectorCount];
            float[] costmph = new float[VectorCount];
            for (int y = 0; y < 8; y++)
            {
                int n = 0;
                for (int x = 0; x < 32; x++)
                {
                    costmph[n] = cos32_1_8[y << 5 | x];
                    n++;
                    if (n == VectorCount)
                    {
                        cos32Vector_1_8[(y << 5 | x) / VectorCount] = new Vector<float>(costmph);
                        n = 0;
                    }
                }
            }
        }

        /// <summary>
        /// pHashっぽいDCT Hashの自前実装
        /// 途中でリサイズしてるけどDCTってリサイズしても意味ないんだよねorz
        /// </summary>
        /// <param name="imgStream">画像ファイルそのもの</param>
        /// <param name="Crop">画像をTwitterの :thumb っぽく正方形に切り抜く</param>
        /// <returns></returns>
        public static long? DCTHash(Stream imgStream, bool Crop = false)
        {
            if(imgStream == null) { return null; }

            Span<float> monoimage = stackalloc float[size * size];
            MonoImage(imgStream, monoimage, Crop);
            var hashbuf = MemoryMarshal.Cast<float, Vector<float>>(monoimage); //モノクロ縮小画像
            if (hashbuf == null) { return null; }
            //DCTやる phashで必要な成分だけ求める
            //http://www.ice.gunma-ct.ac.jp/~tsurumi/courses/ImagePro/DCT/2D_DCT.htm
            Span<float> dctbuf = stackalloc float[64];
            for (int u = 0; u < 8; u++)
            {
                for (int v = 0; v < 8; v++)
                {
                    /*
                    float s = 0;
                    for (int y = 0; y < 32; y++)
                        for (int x = 0; x < 32; x++)
                        {
                            //s += cos32[u + 1, y] * hashbuf[((y << 5) + x) / VectorCount][x % VectorCount] * cos32[v + 1, x];
                            s += cos32[u + 1, y] * hashbuf[((y << 5) + x) >> 2][x & 3] * cos32[v + 1, x];
                        }
                    dctbuf[u * 8 + v] = s * dct32[u, v];
                    */
                    float sum = 0;
                    for (int y = 0; y < 32; y++)
                    {
                        var tosumvec = hashbuf[y * xCount] * cos32Vector_1_8[v * xCount];
                        for (int x = 1; x < xCount; x++)
                        {
                            tosumvec += hashbuf[y * xCount + x] * cos32Vector_1_8[v * xCount + x];
                        }
                        float tosum = 0;
                        for (int i = 0; i < VectorCount; i++)
                        {
                            tosum += tosumvec[i];
                        }
                        sum += tosum * cos32_1_8[u << 5 | y];
                    }
                    dctbuf[(u << 3) | v] = sum; //dct32[u, v]をかけていないので実際の64倍の値
                }
            }
            long ret = 0;
            float ave = 0;
            for (int i = 0; i < dctbuf.Length; i++) 
            {
                ave += dctbuf[i];
            }
            ave /= 64;
            for (int i = 0; i < dctbuf.Length; i++)
            {
                if (dctbuf[i] < ave) { ret |= 1L << i; }
                //ret |= (Math.Sign(dctbuf[i]) & 0x80000000) << 32 >> (63 - i); 
            }
            return ret;
        }

        const int size = 32;
        //static readonly object GdiPlusLock = new object();
        //static readonly byte[] imgbuf = new byte[size * size * 4];
        //static readonly Bitmap miniimage = new Bitmap(size, size, PixelFormat.Format32bppArgb);
        //static readonly Graphics g = Graphics.FromImage(miniimage);
        /// <summary>
        /// 正方形、モノクロの画像32x32)に
        /// </summary>
        /// <param name="imgStream"></param>
        /// <param name="ret"></param>
        /// <param name="Crop"></param>
        static void MonoImage(Stream imgStream, Span<float> ret, bool Crop = false)
        {
            var imgbuf = ArrayPool<byte>.Shared.Rent(size * size * 4);
            //wineでネイティブのgdiplusを使うときはシングルスレッドにしないと落ちる＼(^o^)／
            //なおCPUAffinityも1コアにしないと結局落ちる
            //lock (GdiPlusLock)
            {
                using Bitmap miniimage = new Bitmap(size, size, PixelFormat.Format32bppArgb);
                using Graphics g = Graphics.FromImage(miniimage);
                g.InterpolationMode = System.Drawing.Drawing2D.InterpolationMode.High;  //HighQualityBilinerは非対応←???←initial commit時点でHighになっていたのでとりあえずHighで
                g.PixelOffsetMode = System.Drawing.Drawing2D.PixelOffsetMode.Half;
                using (Image img = Image.FromStream(imgStream))
                {
                    if (Crop)
                    {   //Twitterの正方形切り抜きが微妙にずれているのをどうやって再現するか
                        g.DrawImage(img,
                            new Rectangle(0, 0, size, size),
                            img.Width > img.Height ? (img.Width - img.Height) >> 1 : 0,
                            img.Width < img.Height ? (img.Height - img.Width) >> 1 : 0,
                            Math.Min(img.Width, img.Height),
                            Math.Min(img.Width, img.Height),
                            GraphicsUnit.Pixel);
                    }
                    else { g.DrawImage(img, 0, 0, size, size); }
                }
                //バイト配列に取り出す
                //http://www.84kure.com/blog/2014/07/13/c-%E3%83%93%E3%83%83%E3%83%88%E3%83%9E%E3%83%83%E3%83%97%E3%81%AB%E3%83%94%E3%82%AF%E3%82%BB%E3%83%AB%E5%8D%98%E4%BD%8D%E3%81%A7%E9%AB%98%E9%80%9F%E3%81%AB%E3%82%A2%E3%82%AF%E3%82%BB%E3%82%B9/
                BitmapData imgdata = miniimage.LockBits(new Rectangle(0, 0, size, size), ImageLockMode.ReadOnly, PixelFormat.Format32bppArgb);
                Marshal.Copy(imgdata.Scan0, imgbuf, 0, size * size * 4);
                miniimage.UnlockBits(imgdata);
            }
            //モノクロに変換
            for(int i = 0; i < ret.Length; i++)
            { 
                int PixelIndexShift = i << 2;
                //RとBの係数が逆になっているが今更どうしようもない
                ret[i] = (0.299F * imgbuf[PixelIndexShift] + 0.587F * imgbuf[PixelIndexShift + 1] + 0.114F * imgbuf[PixelIndexShift + 2]);
            }
            ArrayPool<byte>.Shared.Return(imgbuf);
        }
    }
}
