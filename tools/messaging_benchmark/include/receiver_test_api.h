#pragma once

#include "message_receiver_api.h"

#include <boost/config.hpp>


class BOOST_SYMBOL_VISIBLE receiver_test_api
{
public:
    virtual ~receiver_test_api() {}

    virtual std::string name() const                           = 0;
    virtual void        config(const std::string& testConfig)  = 0;
    virtual void        run(message_receiver_api& msgReceiver) = 0;
    virtual std::string result() const                         = 0;
};
