'use strict';

const Strategy = {
  TopicNameStrategy: 'TopicNameStrategy',
  TopicRecordNameStrategy: 'TopicRecordNameStrategy',
  RecordNameStrategy: 'RecordNameStrategy'
};

function getSubject(strategy, topic, isKey, recordName) {
  switch (strategy) {
    case Strategy.TopicNameStrategy:
      return `${topic}-${isKey ? 'key' : 'value'}`;
    case Strategy.TopicRecordNameStrategy:
      if(recordName === null) throw new Error("recordName must be set if using TopicRecordNameStrategy");
      return `${topic}-${recordName}`;
    case Strategy.RecordNameStrategy:
      if(recordName === null) throw new Error("recordName must be set if using RecordNameStrategy");
      return recordName;
  }
}

module.exports = {
  Strategy,
  getSubject
};