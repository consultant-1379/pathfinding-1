package com.distocraft.dc5000.etl.engine.common;

import static org.junit.Assert.assertEquals;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Scanner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * 
 * @author edujmau
 *
 */


public class SequenceTest {
  
  private static File idsInfoFile = null;
  
  private static Field idPoolSize = null;
  
  private static Field singletonSequence;
  
  private static Field resID;
     
  private static Method reserveIDMethod;  
  
  private static Constructor<Sequence> sequence;
  
  private static long reserveId = 123456739;
  
  
  @Before
  public void setUp() throws SecurityException, NoSuchMethodException, IOException, NoSuchFieldException{
    
    
    idPoolSize = Sequence.class.getDeclaredField("IDPOOLSIZE");
    idPoolSize.setAccessible(true);
    
    singletonSequence = Sequence.class.getDeclaredField("singletonSequence");
    singletonSequence.setAccessible(true);
    
    sequence = Sequence.class.getDeclaredConstructor();
    sequence.setAccessible(true);
    
    reserveIDMethod = Sequence.class.getDeclaredMethod("reserveIDs");
    reserveIDMethod.setAccessible(true);
    
    resID = Sequence.class.getDeclaredField("resID");
    resID.setAccessible(true);
    
   
    idsInfoFile = new File("IDs.info");
    writeIdsInfoFile(); 
   
  }

  private void writeIdsInfoFile() throws IOException { 
    if(!idsInfoFile.exists()){
    idsInfoFile.createNewFile(); 
    
  }
 
  final BufferedWriter reserveidWriter = new BufferedWriter(new FileWriter(idsInfoFile));
  reserveidWriter.write(String.valueOf(reserveId));
  reserveidWriter.close();}

  private static long scanFile() throws FileNotFoundException {
  final Scanner scannnedId = new Scanner(idsInfoFile);
  while(scannnedId.hasNext()){
    reserveId = scannnedId.nextLong();
  }
  return reserveId;}

  @Test
  public void reserveIDsTest() throws Exception{
    
    Sequence.instance();
    
    assertEquals("Checking reserve ID",scanFile(),123456789);
    
    
  }
  
  
  @Test(expected=Exception.class)
  public void reserveIDsExceptionTest() throws Exception{
  
    if(idsInfoFile!=null){
      idsInfoFile.delete();   
    }
    
    reserveIDMethod.invoke(Share.instance());
    
  }
  
  @Test
  public void getNewId() throws Exception {
   
    reserveId = 123456739;
    
    writeIdsInfoFile();
    
    final Sequence seq = Sequence.instance();
    
    /*
     * Returns the new ID = reserveid+1 if (this.curID < this.resID)
     */
    
    assertEquals("Checking getNewId Method",seq.getNewId(),scanFile()+1);
    
    /*
     * if (this.curID > this.resID)
     * Returns the new ID = curId+1 from IDs.info file
     */
    reserveId = 123456709;
    
    writeIdsInfoFile();
    
    resID.set(seq, 0);

    assertEquals("Checking getNewId Method",seq.getNewId(),scanFile()-50+1);
    
  }
  
  @Test
  public void instanceTest() throws Exception{
    
    final Sequence seq1 = Sequence.instance();
    
    resID.set(seq1, 0);
    
    final Sequence seq2 = Sequence.instance();
    
    assertEquals("Instance Test",resID.get(seq2),0l);
    
  }

  @After
  public void tearDown(){
   if(idsInfoFile!=null){
     idsInfoFile.delete();
   }
  
    
  }
  

}
