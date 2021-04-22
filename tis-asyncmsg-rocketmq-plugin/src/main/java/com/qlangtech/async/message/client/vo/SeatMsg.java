/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.qlangtech.async.message.client.vo;

import java.io.Serializable;

/*
 * Created by juemingzi on 16/7/22.
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class SeatMsg implements Serializable {

    private static final long serialVersionUID = -7953449146484642641L;

    private String entityId;

    private String orderId;

    private String seatId;

    /*连台号*/
    private String connectCode;

    /*座位编号*/
    private String seatCode;

    /**
     * 桌位状态：1空桌；2占用
     */
    private int status;

    private int orderStatus;

    private int version;

    private long timestamp;

    private long orderOpenTime;

    private int peopleCount;

    private SeatMsg() {
    }

    public static class Builder {

        private SeatMsg seatMsg;

        public Builder() {
            seatMsg = new SeatMsg();
        }

        public Builder entityId(String entityId) {
            seatMsg.entityId = entityId;
            return this;
        }

        public Builder orderId(String orderId) {
            seatMsg.orderId = orderId;
            return this;
        }

        public Builder seatId(String seatId) {
            seatMsg.seatId = seatId;
            return this;
        }

        public Builder status(int status) {
            seatMsg.status = status;
            return this;
        }

        public Builder orderStatus(int orderStatus) {
            seatMsg.orderStatus = orderStatus;
            return this;
        }

        public Builder version(int version) {
            seatMsg.version = version;
            return this;
        }

        public Builder timestamp(long timestamp) {
            seatMsg.timestamp = timestamp;
            return this;
        }

        public Builder orderOpenTime(long orderOpenTime) {
            seatMsg.orderOpenTime = orderOpenTime;
            return this;
        }

        public Builder peopleCount(int peopleCount) {
            seatMsg.peopleCount = peopleCount;
            return this;
        }

        public SeatMsg build() {
            return seatMsg;
        }
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getSeatId() {
        return seatId;
    }

    public void setSeatId(String seatId) {
        this.seatId = seatId;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getOrderOpenTime() {
        return orderOpenTime;
    }

    public void setOrderOpenTime(long orderOpenTime) {
        this.orderOpenTime = orderOpenTime;
    }

    public int getPeopleCount() {
        return peopleCount;
    }

    public void setPeopleCount(int peopleCount) {
        this.peopleCount = peopleCount;
    }

    public int getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(int orderStatus) {
        this.orderStatus = orderStatus;
    }

    public String getConnectCode() {
        return connectCode;
    }

    public void setConnectCode(String connectCode) {
        this.connectCode = connectCode;
    }

    public String getSeatCode() {
        return seatCode;
    }

    public void setSeatCode(String seatCode) {
        this.seatCode = seatCode;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SeatMsg{");
        sb.append("entityId='").append(entityId).append('\'');
        sb.append(", orderId='").append(orderId).append('\'');
        sb.append(", seatId='").append(seatId).append('\'');
        sb.append(", status=").append(status);
        sb.append(", connectCode='").append(connectCode).append('\'');
        sb.append(", seatCode='").append(seatCode).append('\'');
        sb.append(", orderStatus=").append(orderStatus);
        sb.append(", version=").append(version);
        sb.append(", timestamp=").append(timestamp);
        sb.append(", orderOpenTime=").append(orderOpenTime);
        sb.append(", peopleCount=").append(peopleCount);
        sb.append('}');
        return sb.toString();
    }
}
