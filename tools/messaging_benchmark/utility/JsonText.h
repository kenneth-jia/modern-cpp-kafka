#pragma once

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <sstream>
#include <string>
#include <vector>


class JsonText
{
public:
    explicit JsonText(std::string jsonString)
        : _decodeBuf(std::move(jsonString))
    {
        if (_jsonDoc.ParseInsitu(_decodeBuf.data()).HasParseError())
        {
            throw std::runtime_error("Failed to parse string with JSON format!");
        }
    }

    using KeysType = std::vector<std::string>;

    std::int64_t getInt(const KeysType& keys) const { return get<std::int64_t>(keys); }
    std::int64_t getInt(const KeysType& keys, std::int64_t defaultRet) const { try { return getInt(keys); } catch (...) { return defaultRet; } }

    std::string getString(const KeysType& keys) const { return get<std::string>(keys); }
    std::string getString(const KeysType& keys, std::string defaultRet) const { try { return getString(keys); } catch (...) { return defaultRet; } }

private:
    template<typename ValueType>
    static ValueType getValue(rapidjson::Value::ConstMemberIterator jsonDocIter,
                              KeysType::const_iterator              keyBegin,
                              KeysType::const_iterator              keyCurr,
                              KeysType::const_iterator              keyEnd)
    {
        if (keyCurr == keyEnd)
        {
            if constexpr(std::is_same<ValueType, std::string>::value)
            {
                if (!jsonDocIter->value.IsString()) 
                {
                    throw std::invalid_argument("Non-string value type for " + getPath(keyBegin, keyCurr));
                }

                return jsonDocIter->value.GetString();
            }
            else
            {
                if (!jsonDocIter->value.IsInt()) 
                {
                    throw std::invalid_argument("Non-int value type for " + getPath(keyBegin, keyCurr));
                }

                return jsonDocIter->value.GetInt();
            }
        }

        if (!jsonDocIter->value.IsObject())
        {
            throw std::invalid_argument("Failed to find the value for " + getPath(keyBegin, keyCurr + 1));
        }

        const rapidjson::Value::ConstMemberIterator subIter = jsonDocIter->value.FindMember(keyCurr->c_str());

        if (subIter == jsonDocIter->value.MemberEnd())
        {
            throw std::invalid_argument("Failed to find the value for " + getPath(keyBegin, keyCurr + 1));
        }

        return getValue<ValueType>(subIter, keyBegin, ++keyCurr, keyEnd);
    }

    template<typename ValueType>
    ValueType get(const KeysType& keys) const
    {
        KeysType::const_iterator keyBegin = keys.cbegin();
        KeysType::const_iterator keyEnd   = keys.cend();
        KeysType::const_iterator keyCurr  = keyBegin;

        if (keyBegin == keyEnd) throw std::invalid_argument("Null input parameter!");

        const rapidjson::Value::ConstMemberIterator jsonDocIter = _jsonDoc.FindMember(keyCurr->c_str());
        if (jsonDocIter == _jsonDoc.MemberEnd()) throw std::invalid_argument("Failed to find value for " + getPath(keyBegin, keyCurr + 1));

        return getValue<ValueType>(jsonDocIter, keyBegin, ++keyCurr, keyEnd);
    }

    static std::string getPath(KeysType::const_iterator begin, KeysType::const_iterator end)
    {
        std::ostringstream oss;

        for (auto iter = begin; iter < end; ++iter) oss << "/" << *iter;

        return oss.str();
    }

    std::string         _decodeBuf;
    rapidjson::Document _jsonDoc;
};
