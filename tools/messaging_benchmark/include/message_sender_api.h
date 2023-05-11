#pragma once

#include <boost/config.hpp>

#include <functional>
#include <string>
#include <system_error>
#include <utility>


class BOOST_SYMBOL_VISIBLE message_sender_api
{
public:
    using MsgPayload = std::pair<const void*, std::size_t>;
    using DeliveryCb = std::function<void(std::error_code ec)>;
    using MsgSendFn  = std::function<void(MsgPayload, DeliveryCb)>;

    virtual ~message_sender_api() {}

    virtual std::string name() const                            = 0;
    virtual void        config(const std::string& senderConfig) = 0;
    virtual void        send(MsgPayload msg, DeliveryCb cb)     = 0;
};
