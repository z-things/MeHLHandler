/**
 * Created by jacky on 2017/2/4.
 */
'use strict';
var util = require('util');
var uuid = require('node-uuid');
var async = require('async');
var logger = require('./../mlogger/mlogger');
var VirtualDevice = require('./../virtual-device').VirtualDevice;
var TCP_RESPONSE = JSON.stringify({"result": "ok"});
var TCP_HEARTBEAT = "__heartbeat__";
var CONN_TIME_OUT = 2 * 60 * 1000;
var HL_TYPE_ID = '050608070001';
var TEMPORARY_UUID = "xxxx-temporary-uuid-xxxx";
var EVENT_SCHEMAS = {
    device_report: {
        "type": "object",
        "properties": {
            "mac": {"type": "string", "pattern":"^[A-F0-9]{12}$"},
            "dev_type": {"type": "number"},
            "dis_dev_name": {"type": "string"},
            "dis_temp": {"type": "string"},
            "status_onoff": {"type": "number"},
            "temp_heat": {"type": "string"},
            "temp_out": {"type": "string"},
            "temp_comfort": {"type": "string"},
            "temp_energy": {"type": "string"},
            "heat_mode": {"type": "number"},
            "status": {"type": "number"},
            "encrypt": {"type": "string"},
            "temp_heat_default_max":{"type": "string"},
            "temp_heat_default_min":{"type": "string"},
            "timer": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "week": {"type": "number"},
                        "sub_timer": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "index": {"type": "number"},
                                    "time": {"type": "string"},
                                    "temp_heat": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            }
        },
        "required": [
            "mac",//
            "dev_type",//
            "dis_dev_name",//
            "dis_temp",//
            "status_onoff",//
            "temp_heat",//
            "temp_out",//
            "temp_comfort",//
            "temp_energy",//
            "heat_mode",//
            "status",//
            "encrypt"//
        ],
        "additionalProperties": false
    },
    state_report: {
        "type": "object",
        "properties": {
            "mac": {"type": "string", "pattern":"^[A-F0-9]{12}$"},
            "dev_type": {"type": "number"},
            "dis_dev_name": {"type": "string"},
            "dis_temp": {"type": "string"},
            "status_onoff": {"type": "number"},
            "temp_heat": {"type": "string"},
            "temp_out": {"type": "string"},
            "temp_comfort": {"type": "string"},
            "temp_energy": {"type": "string"},
            "heat_mode": {"type": "number"},
            "status": {"type": "number"},
            "temp_heat_default_max":{"type": "string"},
            "temp_heat_default_min":{"type": "string"},
            "encrypt": {"type": "string"},
            "timer": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "week": {"type": "number"},
                        "sub_timer": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "index": {"type": "number"},
                                    "time": {"type": "string"},
                                    "temp_heat": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            }
        },
        "required": [
            "mac"
        ]
    },
    error_report: {
        "type": "object",
        "properties": {
            "mac": {"type": "string", "pattern":"^[A-F0-9]{12}$"},
            "errorTime": {"type": "string"},
            "errorID": {"type": "string"},
            "errorMSG": {"type": "string"}
        },
        "required": [
            "mac", "errorTime", "errorID", "errorMSG"
        ],
        "additionalProperties": false
    }
};
var OPERATION_SCHEMAS = {
    handle: {
        "type": "object",
        "properties": {
            "from": {"type": "string"},
            "data": {
                "type": [
                    "object",
                    "array",
                    "number",
                    "boolean",
                    "string",
                    "null"
                ]
            }
        },
        "required": ["from", "data"]
    },
    disconnected: {
        "type": "object",
        "properties": {
            "from": {"type": "string"}
        },
        "required": ["from"]
    },
    aliveCheck: {
        "type": "object",
        "properties": {
            "from": {"type": "string"}
        },
        "required": ["from"]
    }
};
var getMode = function (mode) {
    return mode === 1 ? "AWAY" : mode === 2 ? "AUTO" : "MANUAL";
};
var convertTimer = function (timers, deviceUuid, deviceType) {
    var newTimers = [];
    if (util.isNullOrUndefined(timers) || !util.isArray(timers)) {
        return null;
    }
    for (var i = 0, len = timers.length; i < len; ++i) {
        var subTimers = timers[i].sub_timer;
        for (var j = 0, lenSub = subTimers.length; j < lenSub; ++j) {
            var subTimer = subTimers[j];
            newTimers.push({
                timerId: uuid.v4(),
                index: subTimer.index,
                name: "thermostat_timer",
                mode: "SERIES",
                interval: 0,
                between: [subTimer.time],
                weekday: [timers[i].week],
                commands: [
                    {
                        uuid: deviceUuid,
                        deviceType: deviceType,
                        cmd: {
                            cmdName: "set_temperature",
                            cmdCode: "0003",
                            parameters: {
                                heat_mode: 2,
                                temp_heat: subTimer.temp_heat
                            }
                        }
                    }
                ]
            });
        }
    }
    return newTimers;
};
function HL_handler(conx, uuid, token, configurator) {
    VirtualDevice.call(this, conx, uuid, token, configurator);
}
util.inherits(HL_handler, VirtualDevice);

/**
 * 远程RPC回调函数
 * @callback onMessage~handle
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "retCode":{number},
 *          "description":{string},
 *          "data":{object}
 *      }
 * }
 */
/**
 * 消息处理
 * @param {object} message :消息体
 * @param {onMessage~handle} peerCallback: 远程RPC回调函数
 * */
HL_handler.prototype.handle = function (message, peerCallback) {
    var self = this;
    var responseMessage = {retCode: 200, description: "Success.", data: TCP_RESPONSE};
    self.messageValidate(message, OPERATION_SCHEMAS.handle, function (error) {
        if (error) {
            responseMessage = error;
            peerCallback(error);
        }
        else {
            var dataBuffer = new Buffer(message.data.data);
            var data = null;
            try {
                var dataString = dataBuffer.toString("utf8");
                data = dataString === TCP_HEARTBEAT ? TCP_HEARTBEAT : JSON.parse(dataString);
                logger.debug("data string:[" + dataString + "]");
            }
            catch (e) {
                logger.warn("Error data string:[" + dataString + "]");
                responseMessage.retCode = 217001;
                responseMessage.description = "Invalid TCP data";
                responseMessage.data = null;
                peerCallback(responseMessage);
                return;
            }

            async.waterfall([
                    function (innerCallback) {
                        var condition = null;
                        if (data === TCP_HEARTBEAT) {
                            condition = {
                                "type.id": HL_TYPE_ID,
                                "extra.connection.socket": message.from
                            }
                        }
                        else {
                            condition = {
                                "type.id": HL_TYPE_ID,
                                "extra.mac": data.mac
                            }
                        }
                        //logger.debug(condition);
                        var msg = {
                            devices: self.configurator.getConfRandom("services.device_manager"),
                            payload: {
                                cmdName: "getDevice",
                                cmdCode: "0003",
                                parameters: condition
                            }
                        };
                        self.message(msg, function (response) {
                            if (response.retCode === 200) {
                                var deviceInfo = response.data;
                                if (util.isArray(response.data)) {
                                    deviceInfo = response.data[0];
                                }
                                innerCallback(null, deviceInfo);
                            } else if (response.retCode === 200003) {
                                innerCallback({errorId: response.retCode, errorMsg: response.description});
                            }
                            else {
                                innerCallback(null, null);
                            }
                        });
                    },
                    function (deviceInfo, innerCallback) {
                        if (!util.isNullOrUndefined(deviceInfo)) {
                            if (deviceInfo.status.network === "DISCONNECTED") {
                                self.message({
                                    devices: self.configurator.getConfRandom("services.event_source"),
                                    payload: {
                                        cmdName: "save",
                                        cmdCode: "0001",
                                        parameters: {
                                            eventTag: "EVENT_DEV_YUEDONG_WATER_UPDATE_NETWORK",
                                            eventData: {
                                                uuid: deviceInfo.uuid,
                                                network: "CONNECTED"
                                            }
                                        }
                                    }
                                });
                            }
                        }
                        if (data === TCP_HEARTBEAT) {
                            responseMessage.data = TCP_HEARTBEAT;
                            if (!util.isNullOrUndefined(deviceInfo)) {
                                var updateMessage = {
                                    "uuid": deviceInfo.uuid,
                                    "status.network": "CONNECTED",
                                    "extra.connection": {
                                        socket: message.from,
                                        lastTime: Date.now()
                                    }
                                };

                                innerCallback("break", {
                                    devices: self.configurator.getConfRandom("services.device_manager"),
                                    payload: {
                                        cmdName: "deviceUpdate",
                                        cmdCode: "0004",
                                        parameters: updateMessage
                                    }
                                });
                            }
                            else {
                                innerCallback("break", null);
                            }
                        }
                        else {
                            innerCallback(null, deviceInfo);
                        }
                    },
                    function (deviceInfo, innerCallback) {
                        self.messageValidate(data, EVENT_SCHEMAS.error_report, function (error) {
                            if (error) {
                                innerCallback(null, deviceInfo);
                            }
                            else {
                                if (!util.isNullOrUndefined(deviceInfo) && !util.isNullOrUndefined(deviceInfo.userId)) {
                                    var event = {
                                        eventTag: "EVENT_DEV_YUEDONG_WATER_EXCEPTION_REPORT",
                                        eventData: {
                                            uuid: deviceInfo.uuid,
                                            errorTime: data.errorTime,
                                            errorID: data.errorID,
                                            errorMSG: data.errorMSG
                                        }
                                    };
                                    innerCallback("break", {
                                        devices: self.configurator.getConfRandom("services.event_source"),
                                        payload: {
                                            cmdName: "save",
                                            cmdCode: "0001",
                                            parameters: event
                                        }
                                    });
                                }
                                else {
                                    innerCallback("break", null);
                                }
                            }
                        });
                    },
                    function (deviceInfo, innerCallback) {
                        self.messageValidate(data, EVENT_SCHEMAS.device_report, function (error) {
                            if (error) {
                                if (util.isNullOrUndefined(deviceInfo)) {
                                    innerCallback({errorId: 217002, errorMsg: "Can not find the device."});
                                }
                                else {
                                    innerCallback(null, deviceInfo);
                                }
                            }
                            else {
                                if (util.isNullOrUndefined(deviceInfo)) {
                                    var newDevice = {
                                        name: data.dis_dev_name,
                                        status: {
                                            switch: data.status_onoff === 0 ? "OFF" : "ON",
                                            network: "CONNECTED"
                                        },
                                        type: {
                                            id: HL_TYPE_ID,
                                            name: "Yuedong-Water",
                                            icon: ""
                                        },
                                        extra: {
                                            mac: data.mac,
                                            dev_type: data.dev_type,
                                            dis_dev_name: data.dis_dev_name,
                                            status_onoff: data.status_onoff,
                                            encrypt: data.encrypt,
                                            items: {
                                                dis_temp: data.dis_temp,
                                                temp_heat: data.temp_heat,
                                                temp_out: data.temp_out,
                                                temp_comfort: data.temp_comfort,
                                                temp_energy: data.temp_energy,
                                                heat_mode: data.heat_mode,
                                                status: data.status
                                            }
                                        }
                                    };
                                    newDevice.extra.timers = convertTimer(data.timer, TEMPORARY_UUID, HL_TYPE_ID);
                                    newDevice.extra["connection"] = {socket: message.from, lastTime: Date.now()};
                                    if(!util.isNullOrUndefined(data.temp_heat_default_max)){
                                        newDevice.extra["temp_heat_default_max"] = data.temp_heat_default_max;
                                    }
                                    if(!util.isNullOrUndefined(data.temp_heat_default_min)){
                                        newDevice.extra["temp_heat_default_min"] = data.temp_heat_default_min;
                                    }
                                    innerCallback("break", {
                                        devices: self.configurator.getConfRandom("services.device_manager"),
                                        payload: {
                                            cmdName: "addDevice",
                                            cmdCode: "0001",
                                            parameters: newDevice
                                        }
                                    });
                                }
                                else {
                                    var updateMessage = {
                                        "uuid": deviceInfo.uuid,
                                        "status.switch": data.status_onoff === 0 ? "OFF" : "ON",
                                        "status.network": "CONNECTED",
                                        "extra.timers": convertTimer(data.timer, deviceInfo.uuid, HL_TYPE_ID),
                                        "extra.mac": data.mac,
                                        "extra.dev_type": data.dev_type,
                                        "extra.dis_dev_name": data.dis_dev_name,
                                        "extra.status_onoff": data.status_onoff,
                                        "extra.encrypt": data.encrypt,
                                        "extra.connection": {
                                            socket: message.from,
                                            lastTime: Date.now()
                                        },
                                        "extra.items.dis_temp": data.dis_temp,
                                        "extra.items.temp_heat": data.temp_heat,
                                        "extra.items.temp_out": data.temp_out,
                                        "extra.items.temp_comfort": data.temp_comfort,
                                        "extra.items.temp_energy": data.temp_energy,
                                        "extra.items.heat_mode": data.heat_mode,
                                        "extra.items.status": data.status
                                    };
                                    if(!util.isNullOrUndefined(data.temp_heat_default_max)){
                                        updateMessage["extra.temp_heat_default_max"] = data.temp_heat_default_max;
                                    }
                                    if(!util.isNullOrUndefined(data.temp_heat_default_min)){
                                        updateMessage["extra.temp_heat_default_min"] = data.temp_heat_default_min;
                                    }
                                    innerCallback("break", {
                                        devices: self.configurator.getConfRandom("services.device_manager"),
                                        payload: {
                                            cmdName: "deviceUpdate",
                                            cmdCode: "0004",
                                            parameters: updateMessage
                                        }
                                    });
                                }
                            }
                        })
                    },
                    function (deviceInfo, innerCallback) {
                        self.messageValidate(data, EVENT_SCHEMAS.state_report, function (error) {
                            if (error) {
                                logger.warn("Invalid data string:[" + dataString + "]");
                                innerCallback({errorId: 217001, errorMsg: "Invalid TCP data"});
                            }
                            else {
                                var updateMessage = {
                                    "uuid": deviceInfo.uuid,
                                    "status.network": "CONNECTED",
                                    "extra.mac": data.mac,
                                    "extra.connection": {
                                        socket: message.from,
                                        lastTime: Date.now()
                                    }
                                };
                                if (!util.isNullOrUndefined(data.status_onoff)) {
                                    updateMessage["status.switch"] = data.status_onoff === 0 ? "OFF" : "ON";
                                    updateMessage["extra.status_onoff"] = data.status_onoff;
                                    if (deviceInfo.status.switch !== updateMessage["status.switch"]) {
                                        self.message({
                                            devices: self.configurator.getConfRandom("services.event_source"),
                                            payload: {
                                                cmdName: "save",
                                                cmdCode: "0001",
                                                parameters: {
                                                    eventTag: "EVENT_DEV_YUEDONG_WATER_POWER_STATUS_REPORT",
                                                    eventData: {
                                                        uuid: deviceInfo.uuid,
                                                        power: updateMessage["status.switch"]
                                                    }
                                                }
                                            }
                                        });
                                    }
                                }
                                if (!util.isNullOrUndefined(data.dev_type)) {
                                    updateMessage["extra.dev_type"] = data.dev_type;
                                }
                                if (!util.isNullOrUndefined(data.encrypt)) {
                                    updateMessage["extra.encrypt"] = data.encrypt;
                                }
                                if (!util.isNullOrUndefined(data.dis_temp)) {
                                    updateMessage["extra.items.dis_temp"] = data.dis_temp;
                                }
                                if (!util.isNullOrUndefined(data.temp_heat)) {
                                    updateMessage["extra.items.temp_heat"] = data.temp_heat;
                                }
                                if (!util.isNullOrUndefined(data.temp_out)) {
                                    updateMessage["extra.items.temp_out"] = data.temp_out;
                                }
                                if (!util.isNullOrUndefined(data.temp_comfort)) {
                                    updateMessage["extra.items.temp_comfort"] = data.temp_comfort;
                                }
                                if (!util.isNullOrUndefined(data.temp_energy)) {
                                    updateMessage["extra.items.temp_energy"] = data.temp_energy;
                                }
                                if (!util.isNullOrUndefined(data.heat_mode)) {
                                    updateMessage["extra.items.heat_mode"] = data.heat_mode;
                                    if (!util.isNullOrUndefined(deviceInfo.extra.items)
                                        && deviceInfo.extra.items.heat_mode !== data.heat_mode) {
                                        self.message({
                                            devices: self.configurator.getConfRandom("services.event_source"),
                                            payload: {
                                                cmdName: "save",
                                                cmdCode: "0001",
                                                parameters: {
                                                    eventTag: "EVENT_DEV_YUEDONG_WATER_HEATING_MODE_REPORT",
                                                    eventData: {
                                                        uuid: deviceInfo.uuid,
                                                        heat_mode: getMode(data.heat_mode)
                                                    }
                                                }
                                            }
                                        });
                                    }
                                }
                                if (!util.isNullOrUndefined(data.status)) {
                                    updateMessage["extra.items.status"] = data.status;
                                    if (!util.isNullOrUndefined(deviceInfo.extra.items)
                                        && deviceInfo.extra.items.status !== data.status) {
                                        self.message({
                                            devices: self.configurator.getConfRandom("services.event_source"),
                                            payload: {
                                                cmdName: "save",
                                                cmdCode: "0001",
                                                parameters: {
                                                    eventTag: "EVENT_DEV_YUEDONG_WATER_HEATING_STATUS_REPORT",
                                                    eventData: {
                                                        uuid: deviceInfo.uuid,
                                                        status: data.status
                                                    }
                                                }
                                            }
                                        });
                                    }
                                }
                                if(!util.isNullOrUndefined(data.temp_heat_default_max)){
                                    updateMessage["extra.temp_heat_default_max"] = data.temp_heat_default_max;
                                }
                                if(!util.isNullOrUndefined(data.temp_heat_default_min)){
                                    updateMessage["extra.temp_heat_default_min"] = data.temp_heat_default_min;
                                }
                                if (!util.isNullOrUndefined(data.timer)) {
                                    var hl_timers = data.timer;
                                    var timers = deviceInfo.extra.timers;
                                    if (util.isNullOrUndefined(timers) || !util.isArray(timers)) {
                                        timers = [];
                                    }
                                    for (var i = 0, hl_len = hl_timers.length; i < hl_len; ++i) {
                                        var hl_timer = hl_timers[i];
                                        var hl_subTimers = hl_timer.sub_timer;
                                        for (var j = 0, lenSub = hl_subTimers.length; j < lenSub; ++j) {
                                            var found = false;
                                            var hl_subTimer = hl_subTimers[j];
                                            for (var k = 0, lenTimers = timers.length; k < lenTimers && !found; ++k) {
                                                var timer = timers[k];
                                                for (var m = 0, lenWeek = timer.weekday.length; m < lenWeek; ++m) {
                                                    if (hl_timer.week === timer.weekday[m] && hl_subTimer.index === timer.index) {
                                                        found = true;
                                                        break;
                                                    }
                                                }
                                                if (found) {
                                                    timer.between = [hl_subTimer.time];
                                                    timer.commands = [
                                                        {
                                                            uuid: deviceInfo.uuid,
                                                            deviceType: HL_TYPE_ID,
                                                            cmd: {
                                                                cmdName: "set_temperature",
                                                                cmdCode: "0003",
                                                                parameters: {
                                                                    heat_mode: 2,
                                                                    temp_heat: hl_subTimer.temp_heat
                                                                }
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                            if (!found) {
                                                timers.push({
                                                    timerId: uuid.v4(),
                                                    index: hl_subTimer.index,
                                                    name: "thermostat_timer",
                                                    mode: "SERIES",
                                                    interval: 0,
                                                    between: [hl_subTimer.time],
                                                    weekday: [hl_timer.week],
                                                    commands: [
                                                        {
                                                            uuid: deviceInfo.uuid,
                                                            deviceType: HL_TYPE_ID,
                                                            cmd: {
                                                                cmdName: "set_temperature",
                                                                cmdCode: "0003",
                                                                parameters: {
                                                                    heat_mode: 2,
                                                                    temp_heat: hl_subTimer.temp_heat
                                                                }
                                                            }
                                                        }
                                                    ]
                                                });
                                            }
                                        }
                                    }
                                    updateMessage["extra.timers"] = timers;
                                }

                                innerCallback("break", {
                                    devices: self.configurator.getConfRandom("services.device_manager"),
                                    payload: {
                                        cmdName: "deviceUpdate",
                                        cmdCode: "0004",
                                        parameters: updateMessage
                                    }
                                });
                            }
                        })
                    }
                ],
                function (error, message) {
                    if (error === "break") {
                        if (!util.isNullOrUndefined(message)) {
                            self.message(message, function (response) {
                                if (response.retCode !== 200) {
                                    responseMessage.retCode = response.retCode;
                                    responseMessage.description = response.description;
                                }
                                peerCallback(responseMessage);
                            })
                        }
                        else {
                            peerCallback(responseMessage);
                        }
                    }
                    else {
                        responseMessage.retCode = error.errorId;
                        responseMessage.description = error.errorMsg;
                        peerCallback(responseMessage);
                    }
                });
        }
    });
};
/**
 * 远程RPC回调函数
 * @callback onMessage~disconnected
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "retCode":{number},
 *          "description":{string},
 *          "data":{object}
 *      }
 * }
 */
/**
 * 处理连接中断
 * @param {object} message :消息体
 * @param {onMessage~disconnected} peerCallback: 远程RPC回调函数
 * */
HL_handler.prototype.disconnected = function (message, peerCallback) {
    var self = this;
    var responseMessage = {retCode: 200, description: "Success.", data: TCP_RESPONSE};
    self.messageValidate(message, OPERATION_SCHEMAS.disconnected, function (error) {
        if (error) {
            responseMessage = error;
            peerCallback(error);
        }
        else {
            var msg = {
                devices: self.configurator.getConfRandom("services.device_manager"),
                payload: {
                    cmdName: "getDevice",
                    cmdCode: "0003",
                    parameters: {
                        "extra.connection.from": message.from
                    }
                }
            };
            self.message(msg, function (response) {
                if (response.retCode === 200) {
                    var deviceInfo = response.data;
                    if (util.isArray(response.data)) {
                        deviceInfo = response.data[0];
                    }
                    if (!util.isNullOrUndefined(deviceInfo)) {
                        if (deviceInfo.status.network === "CONNECTED") {
                            self.message({
                                devices: self.configurator.getConfRandom("services.event_source"),
                                payload: {
                                    cmdName: "save",
                                    cmdCode: "0001",
                                    parameters: {
                                        eventTag: "EVENT_DEV_YUEDONG_WATER_UPDATE_NETWORK",
                                        eventData: {
                                            uuid: deviceInfo.uuid,
                                            network: "DISCONNECTED"
                                        }
                                    }
                                }
                            });
                        }
                        var updateMessage = {
                            devices: self.configurator.getConfRandom("services.device_manager"),
                            payload: {
                                cmdName: "deviceUpdate",
                                cmdCode: "0004",
                                parameters: {
                                    "uuid": deviceInfo.uuid,
                                    "status.network": "DISCONNECTED",
                                    "extra.connection": null
                                }
                            }
                        };
                        self.message(updateMessage, function (response) {
                            if (response.retCode !== 200) {
                                responseMessage.retCode = response.retCode;
                                responseMessage.description = response.description;
                            }
                        })
                    }
                }
            });
            peerCallback(responseMessage);
        }
    });
};

/**
 * 远程RPC回调函数
 * @callback onMessage~disconnect
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "retCode":{number},
 *          "description":{string},
 *          "data":{object}
 *      }
 * }
 */
/**
 * 心跳活性检测
 * @param {object} message :消息体
 * @param {onMessage~aliveCheck} peerCallback: 远程RPC回调函数
 * */
HL_handler.prototype.aliveCheck = function (message, peerCallback) {
    var self = this;
    var responseMessage = {
        retCode: 200,
        description: "Success.",
        data: {
            socket: message.from,
            alive: true
        }
    };
    self.messageValidate(message, OPERATION_SCHEMAS.aliveCheck, function (error) {
        if (error) {
            responseMessage = error;
            peerCallback(error);
        }
        else {
            async.waterfall([
                    function (innerCallback) {
                        var msg = {
                            devices: self.configurator.getConfRandom("services.device_manager"),
                            payload: {
                                cmdName: "getDevice",
                                cmdCode: "0003",
                                parameters: {
                                    "extra.connection.socket": message.from
                                }
                            }
                        };
                        self.message(msg, function (response) {
                            if (response.retCode === 200) {
                                var deviceInfo = response.data;
                                if (util.isArray(response.data)) {
                                    deviceInfo = response.data[0];
                                }
                                innerCallback(null, deviceInfo);
                            } else {
                                responseMessage.data = {
                                    socket: message.from,
                                    alive: false
                                };
                                innerCallback({errorId: response.retCode, errorMsg: response.description});
                            }
                        });
                    },
                    function (deviceInfo, innerCallback) {
                        if (util.isNullOrUndefined(deviceInfo) || util.isNullOrUndefined(deviceInfo.extra.connection)) {
                            responseMessage.data = {
                                socket: message.from,
                                alive: false
                            };
                        }
                        else if (Date.now() - deviceInfo.extra.connection.lastTime > CONN_TIME_OUT) {
                            responseMessage.data = {
                                socket: message.from,
                                alive: false
                            };
                            if (deviceInfo.status.network === "CONNECTED") {
                                self.message({
                                    devices: self.configurator.getConfRandom("services.event_source"),
                                    payload: {
                                        cmdName: "save",
                                        cmdCode: "0001",
                                        parameters: {
                                            eventTag: "EVENT_DEV_YUEDONG_WATER_UPDATE_NETWORK",
                                            eventData: {
                                                uuid: deviceInfo.uuid,
                                                network: "DISCONNECTED"
                                            }
                                        }
                                    }
                                });
                            }
                            var updateMessage = {
                                devices: self.configurator.getConfRandom("services.device_manager"),
                                payload: {
                                    cmdName: "deviceUpdate",
                                    cmdCode: "0004",
                                    parameters: {
                                        "uuid": deviceInfo.uuid,
                                        "status.network": "DISCONNECTED",
                                        "extra.connection": null
                                    }
                                }
                            };
                            self.message(updateMessage, function (response) {
                                if (response.retCode !== 200) {
                                    logger.error(response.retCode, response.description);
                                }
                            });
                        }
                        innerCallback(null);
                    }
                ],
                function (error) {
                    if (error) {
                        logger.error(error.errorId, error.errorMsg);
                    }
                    peerCallback(responseMessage);
                });
        }
    });
};

module.exports = {
    Service: HL_handler,
    OperationSchemas: OPERATION_SCHEMAS
};