﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.AspNetCore.StaticFiles;
using Twigaten.Lib;
using Twigaten.Web.DBHandler;

namespace Twigaten.Web
{
    public static class twimgStatic
    {
        public static readonly HttpClient Http = new HttpClient(new HttpClientHandler()
        {
            UseCookies = false,
            SslProtocols = System.Security.Authentication.SslProtocols.Tls12 | System.Security.Authentication.SslProtocols.Tls13
        })
        {
            DefaultRequestVersion = HttpVersion.Version20
        };
        static readonly FileExtensionContentTypeProvider ExtMime = new FileExtensionContentTypeProvider();
        public static string GetMime(string FileName)
        {
            if (ExtMime.TryGetContentType(FileName, out string mime)) { return mime; }
            else { return "application/octet-stream"; };
        }
        public static readonly DBTwimg DB = DBHandler.DB.DBTwimg;
        public static readonly DBCrawl DBCrawl = DBHandler.DB.DBCrawl;
        public static readonly RemovedMedia Removed = new RemovedMedia();

        /// <summary>
        /// 取得した画像(thumb)を保存 DBはdownloaded_atだけ更新する
        /// </summary>
        public static readonly ActionBlock<(DBTwimg.MediaInfo, byte[])> StoreMediaBlock
            = new ActionBlock<(DBTwimg.MediaInfo MediaInfo, byte[] Bytes)>(
            async (m) =>
        {
            if (0 < await DBCrawl.StoreMedia_downloaded_at(m.MediaInfo.media_id).ConfigureAwait(false))
            {
                try
                {
                    using (var file = File.Create(MediaFolderPath.ThumbPath(m.MediaInfo.media_id, m.MediaInfo.media_url)))
                    {
                        await file.WriteAsync(m.Bytes, 0, m.Bytes.Length).ConfigureAwait(false);
                        await file.FlushAsync().ConfigureAwait(false);
                    }
                    Counter.MediaStored.Increment();
                }
                catch { }
            }
        }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount });

        /// <summary>
        /// 取得したアイコン(profile_image)を保存 DBはupdated_atだけ更新する
        /// </summary>
        public static readonly ActionBlock<(DBTwimg.ProfileImageInfo, byte[])> StoreProfileImageBlock
            = new ActionBlock<(DBTwimg.ProfileImageInfo ProfileImageInfo, byte[] Bytes)>(
            async (u) =>
        {
            //初期アイコンはいろいろ面倒なのでここではやらない
            if (!u.ProfileImageInfo.is_default_profile_image
                && 0 < await DBCrawl.StoreUser_updated_at(u.ProfileImageInfo.user_id).ConfigureAwait(false))
            {
                try
                {
                    using (var file = File.Create(MediaFolderPath.ProfileImagePath(u.ProfileImageInfo.user_id, u.ProfileImageInfo.is_default_profile_image, u.ProfileImageInfo.profile_image_url)))
                    {
                        await file.WriteAsync(u.Bytes, 0, u.Bytes.Length).ConfigureAwait(false);
                        await file.FlushAsync().ConfigureAwait(false);
                    }
                    Counter.MediaStored.Increment();
                }
                catch { }
            }
        }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount });
    }
}
