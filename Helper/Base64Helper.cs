using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Common.Helpers
{
    /// <summary>
    /// Helper class for Base64 encoding and decoding operations
    /// </summary>
    public static class Base64Helper
    {
        /// <summary>
        /// Encodes a string to Base64
        /// </summary>
        /// <param name="plainText">The plain text to encode</param>
        /// <returns>Base64 encoded string</returns>
        public static string EncodeToBase64(string plainText)
        {
            if (string.IsNullOrEmpty(plainText))
            {
                return string.Empty;
            }

            byte[] plainTextBytes = Encoding.UTF8.GetBytes(plainText);
            return Convert.ToBase64String(plainTextBytes);
        }

        /// <summary>
        /// Decodes a Base64 encoded string
        /// </summary>
        /// <param name="base64EncodedData">The Base64 encoded string</param>
        /// <returns>Decoded string</returns>
        public static string DecodeFromBase64(string base64EncodedData)
        {
            if (string.IsNullOrEmpty(base64EncodedData))
            {
                return string.Empty;
            }

            try
            {
                byte[] base64EncodedBytes = Convert.FromBase64String(base64EncodedData);
                return Encoding.UTF8.GetString(base64EncodedBytes);
            }
            catch (FormatException)
            {
                // Handle invalid Base64 input
                return string.Empty;
            }
        }

        /// <summary>
        /// Checks if a string is a valid Base64 encoded string
        /// </summary>
        /// <param name="base64String">The string to check</param>
        /// <returns>True if the string is valid Base64, false otherwise</returns>
        public static bool IsBase64String(string base64String)
        {
            if (string.IsNullOrEmpty(base64String) || base64String.Length % 4 != 0
                || base64String.Contains(" ") || base64String.Contains("\t") || base64String.Contains("\r") || base64String.Contains("\n"))
            {
                return false;
            }

            try
            {
                Convert.FromBase64String(base64String);
                return true;
            }
            catch (FormatException)
            {
                return false;
            }
        }

        /// <summary>
        /// Encodes byte array to Base64
        /// </summary>
        /// <param name="bytes">Byte array to encode</param>
        /// <returns>Base64 encoded string</returns>
        public static string EncodeToBase64(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0)
            {
                return string.Empty;
            }

            return Convert.ToBase64String(bytes);
        }

        /// <summary>
        /// Decodes Base64 encoded string to byte array
        /// </summary>
        /// <param name="base64EncodedData">The Base64 encoded string</param>
        /// <returns>Decoded byte array</returns>
        public static byte[] DecodeFromBase64ToBytes(string base64EncodedData)
        {
            if (string.IsNullOrEmpty(base64EncodedData))
            {
                return Array.Empty<byte>();
            }

            try
            {
                return Convert.FromBase64String(base64EncodedData);
            }
            catch (FormatException)
            {
                return Array.Empty<byte>();
            }
        }

        /// <summary>
        /// Generates a secure random password that meets standard security requirements
        /// </summary>
        /// <param name="length">Length of the password (default: 12, should be between 8-20)</param>
        /// <returns>A secure password that contains lowercase, uppercase, numbers and special characters</returns>
        public static string GenerateSecurePassword(int length = 12)
        {
            // Validate input length
            if (length < 8 || length > 20)
            {
                length = 12; // Default to 12 if out of range
            }

            // Character sets
            const string lowerChars = "abcdefghijklmnopqrstuvwxyz";
            const string upperChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            const string numberChars = "0123456789";
            const string specialChars = "!@#$%&*";

            // Ensure we have at least one of each required character type
            using var rng = RandomNumberGenerator.Create();
            var password = new StringBuilder();

            // Add one character from each required set
            password.Append(GetRandomChar(lowerChars, rng));
            password.Append(GetRandomChar(upperChars, rng));
            password.Append(GetRandomChar(numberChars, rng));
            password.Append(GetRandomChar(specialChars, rng));

            // Fill the rest with random characters from all allowed sets
            string allChars = lowerChars + upperChars + numberChars + specialChars;
            for (int i = 4; i < length; i++)
            {
                password.Append(GetRandomChar(allChars, rng));
            }

            // Shuffle the password to make it more random
            return ShuffleString(password.ToString(), rng);
        }

        /// <summary>
        /// Generates a secure random password and returns both plain and Base64 encoded versions
        /// </summary>
        /// <param name="length">Length of the password (default: 12, should be between 8-20)</param>
        /// <returns>A tuple containing the plain password and its Base64 encoded version</returns>
        public static (string PlainPassword, string EncodedPassword) GenerateAndEncodePassword(int length = 12)
        {
            string plainPassword = GenerateSecurePassword(length);
            string encodedPassword = EncodeToBase64(plainPassword);
            return (plainPassword, encodedPassword);
        }

        // Helper method to get a random character from a string
        private static char GetRandomChar(string charSet, RandomNumberGenerator rng)
        {
            byte[] buffer = new byte[4];
            rng.GetBytes(buffer);
            int index = BitConverter.ToInt32(buffer, 0) % charSet.Length;
            return charSet[Math.Abs(index)];
        }

        // Helper method to shuffle a string using Fisher-Yates algorithm
        private static string ShuffleString(string text, RandomNumberGenerator rng)
        {
            char[] array = text.ToCharArray();
            int n = array.Length;

            while (n > 1)
            {
                byte[] box = new byte[4];
                rng.GetBytes(box);
                int k = Math.Abs(BitConverter.ToInt32(box, 0)) % n;
                n--;
                char temp = array[n];
                array[n] = array[k];
                array[k] = temp;
            }

            return new string(array);
        }
    }
}
