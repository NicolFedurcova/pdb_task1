package sk.upjs.gursky.pdb;

import sk.upjs.gursky.bplustree.BPTree;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class UnclusteredBPTreeSalary extends BPTree<SalaryKey, SalaryOffsetEntry> {

    public static final long serialVersionUID = 7839365898122602735L;
    public static final File INDEX_FILE = new File("person.unclustered.tree");
    public static final File INPUT_DATA_FILE = new File("person.tab");

    public UnclusteredBPTreeSalary(){
        super(SalaryOffsetEntry.class, INDEX_FILE);
    }

    public static UnclusteredBPTreeSalary createByBulkLoading() throws IOException {
        long start = System.nanoTime();
        UnclusteredBPTreeSalary tree = new UnclusteredBPTreeSalary();

        RandomAccessFile raf = new RandomAccessFile(INPUT_DATA_FILE, "r");

        FileChannel channel = raf.getChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(4096);

        List<SalaryOffsetEntry> entries = new ArrayList<>();
        long fileSize = INPUT_DATA_FILE.length();
        for (int offset = 0; offset < fileSize; offset+=4096) {
            System.out.println("procesing page " + (offset / 4096));

            buffer.clear();
            channel.read(buffer, offset);
            buffer.rewind();
            int numberOfRecords = buffer.getInt();//otazka, jak sa do bufera dostane info o pocte zaznamov???
            for (int record = 0; record < numberOfRecords; record++) {
                PersonEntry entry = new PersonEntry();
                entry.load(buffer);

                long entryOffset = offset+4+record*entry.getSize();
                SalaryOffsetEntry item = new SalaryOffsetEntry(entry.salary, entryOffset);
                entries.add(item);
            }
        }
        Collections.sort(entries);
        tree.openAndBatchUpdate(entries.iterator(), entries.size());
        channel.close();
        raf.close();
        System.out.println("unclustered index created in: " + (System.nanoTime() - start) / 1_000_000.0 + "ms");

        return tree;
    }

    public List<PersonEntry> unclusteredSalaryIntervalQuery(SalaryKey low, SalaryKey high) throws IOException {
        List<SalaryOffsetEntry> references = intervalQuery(low, high);
        List<PersonEntry> result = new LinkedList<>();

        RandomAccessFile raf = new RandomAccessFile(INPUT_DATA_FILE, "r");

        FileChannel channel = raf.getChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(4096);

        int accessesToDisk = 0;
        for (SalaryOffsetEntry ref : references) {

            long position = ref.offset;
            long offsetOfPage = 4096 * (position / 4096);
            buffer.clear();
            channel.read(buffer, offsetOfPage); //do buffera načítame stránku - podľa zaciatoc  offset

            accessesToDisk++;
            buffer.rewind();
            long entryIntPageOffset = position - offsetOfPage; //kde sa mam posunut vramci buffera
            buffer.position((int) (entryIntPageOffset));
            PersonEntry entry = new PersonEntry();
            entry.load(buffer);
            result.add(entry);

        }

        channel.close();
        raf.close();
        System.out.println("Acecsses to disk: " + accessesToDisk);
        return result;


    }



}
