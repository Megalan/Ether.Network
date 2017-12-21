using System;
using System.Net.Sockets;

namespace Ether.Network.Interfaces
{
    internal interface IAsyncUserToken
    {
        // Header

        int ReceivedHeaderBytesCount { get; set; }

        byte[] MessageHeaderData { get; set; }

        int? MessageSize { get; set; }

        int DataCursorIndex { get; set; }

        // Message

        int ReceivedMessageBytesCount { get; set; }

        byte[] MessageData { get; set; }

        // ---------------

        int DataStartOffset { get; set; }

        int NextReceiveOffset { get; set; }

        int TotalReceivedDataSize { get; set; }

        byte[] TempData { get; set; }

        Socket Socket { get; set; }

        Action<byte[]> MessageHandler { get; set; }
    }
}
