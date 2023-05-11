#pragma once

#include <boost/config.hpp>

#include <functional>
#include <string>
#include <utility>
#include <vector>


class BOOST_SYMBOL_VISIBLE message_receiver_api
{
public:
    using MsgPayload = std::pair<const void*, std::size_t>;
    using ReceivedCb = std::function<void(MsgPayload)>;

    virtual ~message_sender_api() {}

    virtual std::string name() const                              = 0;
    virtual void        config(const std::string& receiverConfig) = 0;
    virtual void        run(ReceivedCb cb)                        = 0;
};
