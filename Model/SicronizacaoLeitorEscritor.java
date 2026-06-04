package Model;

import java.util.concurrent.Semaphore;

public class SicronizacaoLeitorEscritor {
     private static Semaphore regicaoCritica;
     private static Semaphore livro; 
     private static Semaphore fila;
     private static int numeroLeitores; 
     static {
          regicaoCritica = new Semaphore(1);
          livro = new Semaphore(1);
          fila = new Semaphore(1, true);
          numeroLeitores = 0;
     }
     
     public static void deixarBaseDadosLeitor() {
            downRc();
               numeroLeitores--;
               if(numeroLeitores == 0) {
                     upLivro();
               }
            upRc();
     }

     public static void editarBaseDadosEscritor() {
          downfila();
            downLivro();
     }

     public static void deixarBaseDadosEscritor() {
         upfila();
         upLivro(); 
     } 

     public static void acessarBaseDadosLeitor() {
            downfila();
             downRc();
              numeroLeitores++;  
              if(numeroLeitores == 1) {
                 downLivro();
              }
             upRc();
           upfila();
     }

     private static void downRc() {
           try {
                regicaoCritica.acquire();
           } catch(Exception ex) {
                ex.printStackTrace(); 
           }       
     }

     private static void upRc() {
            regicaoCritica.release();
     }
 
     private static void upLivro() {
            livro.release();
     }

     private static void downfila() {
           try {
                fila.acquire();
           } catch(Exception ex) {
                ex.printStackTrace(); 
           }       
     }

     private static void upfila() {
             fila.release();
     }

     private static void downLivro() {
           try {
                livro.acquire();
           } catch(Exception ex) {
                ex.printStackTrace(); 
           }       
     } 
}