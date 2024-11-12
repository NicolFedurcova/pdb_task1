package sk.upjs.gursky.pdb;

import sk.upjs.gursky.bplustree.BPTree;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class UnclusteredBPTree extends BPTree<PersonStringKey, SurnameAndOffsetEntry> {

    public static final long serialVersionUID = 9219579263132867697L;
    public static final File INDEX_FILE = new File("person.unclustered.tree");
    public static final File INPUT_DATA_FILE = new File("person.tab");


    public UnclusteredBPTree() throws IOException {
        super(SurnameAndOffsetEntry.class, INDEX_FILE); //index, subor na uloz stromu, subor na uloz metadat
    }

    public static UnclusteredBPTree createByBulkLoading() throws IOException {
        long startTime = System.nanoTime();
        UnclusteredBPTree tree = new UnclusteredBPTree();


        RandomAccessFile raf = new RandomAccessFile(INPUT_DATA_FILE, "r");

        FileChannel channel = raf.getChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(4096);

        List<SurnameAndOffsetEntry> entries = new ArrayList<>();
        long filesize = INPUT_DATA_FILE.length();

        //citam si po strankach person.tab a precitam cleu stranku z pov suboru do buffera
        //precitam si pocet ludi v jednej stanke
        //vyrobim surnameand offset entry
        for (int offset = 0; offset < filesize; offset += 4096) {
            System.out.println("procesing page " + (offset / 4096));

            buffer.clear();
            channel.read(buffer, offset);
            buffer.rewind();
            int numberOfRecords = buffer.getInt();
            for (int i = 0; i < numberOfRecords; i++) {
                PersonEntry entry = new PersonEntry();
                entry.load(buffer);
                //precitali sme si cely zaznamcek

                //z buffera mam cely eńtry ale nie offset
                //offset si vypocitame, z povodneho suboru ako daleko od zaciatku je
                //vieme offet stranky, number of records a velkost person entry
                //na zaciatku stranky vieme pocet zaznamov = int o 4 bajtoch
                //i ty entry teraz citame
                long entryOffset = offset+4+entry.getSize()*i;
                SurnameAndOffsetEntry item = new SurnameAndOffsetEntry(entry.surname, entryOffset);
                entries.add(item);//ludi pchame neusporiadane do jedneho pola == je to cele v ramke

            }

        }

        Collections.sort(entries);
        tree.openAndBatchUpdate(entries.iterator(), entries.size()); //listy su plne na 100%
        channel.close();
        raf.close();
        System.out.println("unclustered index created in: " + (System.nanoTime() - startTime) / 1_000_000.0 + "ms");

        return tree;

    }

    //prehladavanie celej tabulky podla intervalu v neklastrovanom strome
    //z listov pojdeme do externej štruktury

    public List<PersonEntry> unclusteredIntervalQuery(PersonStringKey low, PersonStringKey high ) throws IOException {
        List<SurnameAndOffsetEntry> references = intervalQuery(low,high);//dame si do ramky co najdeme == 1. krok
        //budeme citat na preskacku, skakat po externej strukture

        RandomAccessFile raf = new RandomAccessFile(INPUT_DATA_FILE, "r");

        FileChannel channel = raf.getChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
        List<PersonEntry> result = new LinkedList<>();

        int accessesToDisk = 0;
        for(SurnameAndOffsetEntry ref: references){
            //v ref.offset j emiesto kde sa v subore zaznam nachadza
            //vieme sa zanznamu spytat na dlzku
            //musime z disku citat len jednu stranku, musiem mu povedat zacitao k stranky nie offset
            //1. vypocitat zaciatok stranky pre dany offset
            //2. do byte buffera naloadujem stranku
            //3. v bytebufferi sa previniem na zaznam  --> position
            //4. precitam si zaznam
            //personentry.getSize --> z person tab

            long position = ref.offset;
            long offsetOfPage = 4096*(position/4096);
            buffer.clear();
            channel.read(buffer, offsetOfPage); //do buffera načítame stránku - podľa zaciatoc  offset
            accessesToDisk++;
            long entryIntpageOffset = position-offsetOfPage; //kde s amam posunut vramci buffera
            buffer.position((int)(entryIntpageOffset));
            PersonEntry entry = new PersonEntry();
            entry.load(buffer);
            result.add(entry);
            //most stupid version -- s kazdou referenciou bezi na disk
        }

        channel.close();
        raf.close();
        System.out.println("Acecsses to disk: " + accessesToDisk);
        return result;



    }



}
