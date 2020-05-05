import * as avsc from "avsc";

//TODO: please fix me!!!
export default class MsgTypeResolver extends avsc.types.LogicalType {
  static NAMESPACE = "com.liveperson.maven.schema";
  constructor(props) {
    super(props);
  }

  _fromValue(val) {
    // This is the easy case, we convert the wrapped value to our representation.
    console.log(val.msgData.type);
    // console.log(val.msgData.unwrap());
    const result = val.msgData;
    return Object.assign({}, val, {
      msgData: {
        ...result
      }
    });
  }

  _toValue(any) {
    // Here we do a little more work to wrap the value based on its `type` field.
    const result = any;
    if (result.messageDetails.msgType === MsgContentTypeEnum.RichContent) {
      result.msgData = {
        [`${NAMESPACE}.${RayRichContentData.name}`]: any.msgData
      };
    } else {
      result.msgData = {
        [`${NAMESPACE}.${RayTextData.name}`]: any.msgData
      };
    }
    return result;
  }

  _resolve(type) {
    // if (avsc.Type.isType(type, 'null', 'TextData', 'RichContentData')) {
    return this._fromValue;
    // }
    // return null;
  }
}
