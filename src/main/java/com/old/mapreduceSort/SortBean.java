package com.old.mapreduceSort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description
 * @Package com.old.mapreduceSort
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/13 12:42
 */

public class SortBean implements WritableComparable<SortBean> {
    private String word;
    private int num;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return word + "\t" + num;
    }

    /**
     * 实现序列化
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(num);
    }

    /**
     * 实现反序列化
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = dataInput.readUTF();
        this.num = dataInput.readInt();
    }

    /**
     * 实现比较器
     * 先按照字母word顺序排列，如果字母顺序相同，使用第二列num升序排列
     */
    @Override
    public int compareTo(SortBean o) {
        int result = this.word.compareTo(o.getWord());
        if (result == 0) {
            return this.num - o.getNum();
        }
        return result;
    }
}
