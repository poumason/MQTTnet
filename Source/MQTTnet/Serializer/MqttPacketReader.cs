using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet.Channel;
using MQTTnet.Exceptions;
using MQTTnet.Internal;

namespace MQTTnet.Serializer
{
    public static class MqttPacketReader
    {
        public static async Task<MqttFixedHeader> ReadFixedHeaderAsync(IMqttChannel channel, byte[] fixedHeaderBuffer, byte[] singleByteBuffer, CancellationToken cancellationToken)
        {
            // The MQTT fixed header contains 1 byte of flags and at least 1 byte for the remaining data length.
            // So in all cases at least 2 bytes must be read for a complete MQTT packet.
            // async/await is used here because the next packet is received in a couple of minutes so the performance
            // impact is acceptable according to a useless waiting thread.
            var buffer = fixedHeaderBuffer;
            var totalBytesRead = 0;

            while (totalBytesRead < buffer.Length)
            {
                int bytesRead = 0;

                try
                {
                    bytesRead = await channel.ReadAsync(buffer, totalBytesRead, buffer.Length - totalBytesRead, cancellationToken).ConfigureAwait(false);
                }
                catch (IOException)
                {
                    // could be remote server close connection to trigger exception
                    break;
                }

                // operation has been canceled
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                // nothing to read
                if (bytesRead <= 0)
                {
                    break;
                }

                totalBytesRead += bytesRead;
            }

            if (totalBytesRead < buffer.Length)
            {
                return null;
            }

            var hasRemainingLength = buffer[1] != 0;
            if (!hasRemainingLength)
            {
                return new MqttFixedHeader(buffer[0], 0);
            }

#if WINDOWS_UWP
            // UWP will have a dead lock when calling this not async.
            var readBodyResult = await ReadBodyLengthAsync(channel, buffer[1], singleByteBuffer, cancellationToken).ConfigureAwait(false);
#else
            // Here the async/await pattern is not used becuase the overhead of context switches
            // is too big for reading 1 byte in a row. We expect that the remaining data was sent
            // directly after the initial bytes. If the client disconnects just in this moment we
            // will get an exception anyway.
            var readBodyResult = await ReadBodyLengthAsync(channel, buffer[1], singleByteBuffer, cancellationToken).ConfigureAwait(false);
#endif
            if (!readBodyResult.Item1)
            {
                return null;
            }

            var bodyLength = readBodyResult.Item2;
            return new MqttFixedHeader(buffer[0], bodyLength);
        }

#if !WINDOWS_UWP
        private static async Task<Tuple<bool, int>> ReadBodyLengthAsync(IMqttChannel channel, byte initialEncodedByte, byte[] singleByteBuffer, CancellationToken cancellationToken)
        {
            var offset = 0;
            var multiplier = 128;
            var value = initialEncodedByte & 127;
            int encodedByte = initialEncodedByte;
            bool isSuccess = true;

            while ((encodedByte & 128) != 0)
            {
                offset++;
                if (offset > 3)
                {
                    throw new MqttProtocolViolationException("Remaining length is invalid.");
                }

                cancellationToken.ThrowIfCancellationRequested();

                var readByteResult = await ReadByteAsync(channel, singleByteBuffer, cancellationToken);
                if (!readByteResult.Item1)
                {
                    isSuccess = false;
                    break;
                }

                encodedByte = readByteResult.Item2;

                value += (byte)(encodedByte & 127) * multiplier;
                multiplier *= 128;
            }

            return Tuple.Create(isSuccess, value);
        }

        private static async Task<Tuple<bool, byte>> ReadByteAsync(IMqttChannel channel, byte[] singleByteBuffer, CancellationToken cancellationToken)
        {
            int readCount = 0;

            try
            {
                readCount = await channel.ReadAsync(singleByteBuffer, 0, 1, cancellationToken).ConfigureAwait(false);
            }
            catch (IOException)
            {
                return Tuple.Create(false, (byte)0);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return Tuple.Create(false, (byte)0);
            }

            if (readCount <= 0)
            {
                return Tuple.Create(false, (byte)0);
            }

            return Tuple.Create(true, singleByteBuffer[0]);
        }

#else
        
        private static async Task<Tuple<bool, int>> ReadBodyLengthAsync(IMqttChannel channel, byte initialEncodedByte, byte[] singleByteBuffer, CancellationToken cancellationToken)
        {
            var offset = 0;
            var multiplier = 128;
            var value = initialEncodedByte & 127;
            int encodedByte = initialEncodedByte;
            bool isSuccess = true;

            while ((encodedByte & 128) != 0)
            {
                offset++;
                if (offset > 3)
                {
                    throw new MqttProtocolViolationException("Remaining length is invalid.");
                }

                cancellationToken.ThrowIfCancellationRequested();

                var readResult = await ReadByteAsync(channel, singleByteBuffer, cancellationToken).ConfigureAwait(false);
                if (!readResult.Item1)
                {
                    isSuccess = false;
                    break;
                }

                encodedByte = readResult.Item2;

                value += (byte)(encodedByte & 127) * multiplier;
                multiplier *= 128;
            }

            return Tuple.Create(isSuccess, value);
        }

        private static async Task<Tuple<bool, byte>> ReadByteAsync(IMqttChannel channel, byte[] singleByteBuffer, CancellationToken cancellationToken)
        {
            int readCount = 0;
            try
            {
                readCount = await channel.ReadAsync(singleByteBuffer, 0, 1, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                return Tuple.Create(false, (byte)0);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return Tuple.Create(false, (byte)0);
            }

            if (readCount <= 0)
            {
                return Tuple.Create(false, (byte)0);
            }

            return Tuple.Create(true, singleByteBuffer[0]);
        }

#endif
    }
}
