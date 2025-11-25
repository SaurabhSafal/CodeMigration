using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;

namespace DataMigration.Helper
{
    public class AesEncryptionService
    {
        private readonly string _key;

        public AesEncryptionService()
        {
            _key = "ThisIsA32CharLongEncryptionKey!!";

            if (string.IsNullOrWhiteSpace(_key) || _key.Length != 32)
                throw new ArgumentException("Encryption key must be exactly 32 characters for AES-256.");
        }

        public string Encrypt(string plainText)
        {
            using var aes = Aes.Create();
            aes.Key = Encoding.UTF8.GetBytes(_key);
            aes.GenerateIV();

            using var encryptor = aes.CreateEncryptor();
            byte[] plainBytes = Encoding.UTF8.GetBytes(plainText);
            byte[] cipherBytes = encryptor.TransformFinalBlock(plainBytes, 0, plainBytes.Length);

            byte[] result = new byte[aes.IV.Length + cipherBytes.Length];
            Buffer.BlockCopy(aes.IV, 0, result, 0, aes.IV.Length);
            Buffer.BlockCopy(cipherBytes, 0, result, aes.IV.Length, cipherBytes.Length);

            return Convert.ToBase64String(result);
        }

        public string Decrypt(string cipherTextBase64)
        {
            byte[] fullCipher = Convert.FromBase64String(cipherTextBase64);
            if (fullCipher.Length < 16)
                throw new ArgumentException("Invalid cipher text. Not enough data for IV.");
            using var aes = Aes.Create();
            aes.Key = Encoding.UTF8.GetBytes(_key);

            byte[] iv = new byte[aes.BlockSize / 8];
            byte[] cipher = new byte[fullCipher.Length - iv.Length];

            Buffer.BlockCopy(fullCipher, 0, iv, 0, iv.Length);
            Buffer.BlockCopy(fullCipher, iv.Length, cipher, 0, cipher.Length);

            aes.IV = iv;

            using var decryptor = aes.CreateDecryptor();
            byte[] plainBytes = decryptor.TransformFinalBlock(cipher, 0, cipher.Length);

            return Encoding.UTF8.GetString(plainBytes);
        }

        public static string ComputeSha256Hash(string input)
        {
            using var sha256 = SHA256.Create();
            byte[] bytes = Encoding.UTF8.GetBytes(input);
            byte[] hashBytes = sha256.ComputeHash(bytes);
            return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant(); // 64-char
        }
    }
}
