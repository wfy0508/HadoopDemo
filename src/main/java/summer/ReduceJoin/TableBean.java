package summer.ReduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author summer
 * @title: TableBean
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-20 10:26
 */
public class TableBean implements Writable {
    /**
     * 定义Bean的结构
     * id: 订单id
     * pid: 商品编号
     * pName：商品名称
     * amount：商品价格
     * flag：标记是什么表名
     */
    private String id;
    private String pid;
    private String pName;
    private int amount;
    private String flag;

    public TableBean() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getFlag() {
        return flag;
    }

    @Override
    public String toString() {
        return id + "\t" + pName + "\t" + amount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeUTF(pName);
        out.writeInt(amount);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.pName = in.readUTF();
        this.amount = in.readInt();
        this.flag = in.readUTF();
    }
}
