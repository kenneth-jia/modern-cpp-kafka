#pragma once

#include "message_sender_api.h"

#include <boost/config.hpp>
#include <boost/shared_ptr.hpp>

#include <list>


class BOOST_SYMBOL_VISIBLE sender_test_api
{
public:
    virtual ~sender_test_api() {}

    using MsgSenders = std::list<boost::shared_ptr<message_sender_api>>;

    virtual std::string name() const                          = 0;
    virtual void        config(const std::string& testConfig) = 0;
    virtual void        run(MsgSenders& msgSenders)           = 0;
    virtual std::string result() const                        = 0;
};

