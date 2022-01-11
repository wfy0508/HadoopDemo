package com.old.mapreduceTopN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description
 * @Package com.old.mapreduceTopN
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/17 14:29
 */

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Double price;

    public String getOrderId() {
        return orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }

    /**
     * 指定排序规则
     */
    @Override
    public int compareTo(OrderBean o) {
        // 先比较订单ID，如果一直对相同ID的金额降序排列
        int i = this.orderId.compareTo(o.orderId);
        if (i == 0) {
            i = this.price.compareTo(o.price) * -1;
        }
        return i;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }
}
