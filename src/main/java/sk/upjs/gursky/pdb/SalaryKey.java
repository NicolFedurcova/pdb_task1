package sk.upjs.gursky.pdb;

import sk.upjs.gursky.bplustree.BPKey;

import java.nio.ByteBuffer;

public class SalaryKey  implements BPKey<SalaryKey> {

    private static final long serialVersionUID = 8569190205446731174L;
    private int key;

    public SalaryKey(int key) {
        this.key = key;
    }

    public SalaryKey() {
    }

    @Override
    public void load(ByteBuffer bb) {
        bb.getInt();
    }

    @Override
    public void save(ByteBuffer bb) {
        bb.putInt(key);
    }

    @Override
    public int getSize() {
        return 4; //lebo int ma 4 bajty
    }

    @Override
    public int compareTo(SalaryKey o) {
        if(this.key < o.key){
            return -1;
        }
        if (this.key > o.key) {
            return 1;
        } else {
            return 0;
        }
    }
}
