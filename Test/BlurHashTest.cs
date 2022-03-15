using Blurhash.ImageSharp;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using System.Threading.Tasks;
using Twigaten.Lib.BlurHash;
using Xunit;

namespace Test
{
    public class BlurHashTest
    {
        [Fact]
        public async Task EncodeTest()
        {
            var encoder = new Encoder(new BasisCache());

            var image = await Image.LoadAsync<Rgb24>("1233360707896238080.jpg");

            var encoded = encoder.Encode(image, 9, 9);
            //Result of float non-vector version
            Assert.Equal(@"|cPixSOsi_n%XmkqWVj[bH1kWrW;ayaKaKjZaejZG^rXtQkCiwnioLj[jaQTxti^a|XSXSbHbHbHxDo}X8e:j[jZe.n%fQ%gRjX9f8i{jFf7ayjZt7VtVsaykWbbbbbbbGk=V[j?kVofkCjZoLayR6baozofaejbjZjFj[", encoded);
        }
    }
}