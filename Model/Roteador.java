/************************************************************************************************
*Autor............: Gustavo Henrique Oliveira Fernandes
*Matricula........: 202410104
*Inicio...........: 29/08/2026
*Ultima alteracao.: 29/08/2026
*Nome.............: Roteador
*Funcao...........: representar um roteador em uma simulacao de rede utilizando threads, 
*                   com controle de envio de pacotes por algoritmo de inundacao
**************************************************************************************************/

package Model;//essa classe eh um prototipo, ainda em desenvolvimento

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import Controller.ControllerDeMover;

public class Roteador extends Thread {

   private Fila<Pacote> fila; //fila de pacote  
   private Semaphore mutex;// controle de exclusao mutua
   private Semaphore mutexFila;// controle de sincronizacao da fila
   private RoteadoresAdjacentes interfaceSaida;// roteadores adjacentes
   private Posicao localizacao;// posicao no espaco
   private int id_identificao;// identificador do roteador
   private ControllerDeMover ponteiro;// controlador de envio
   private TabelaDistancia tabela;
   private Mapeamento HashTable;
   //veja que o inicio modela o envio do ping de estimativa inicial do custo do enlace adjacente, enquanto timeLimmit indica o envio periodico de pacotes de controle para preenchimento da tabela
   /********************************************************************************************
   * Metodo: Roteador
   * Funcao: inicializar o roteador com seus atributos basicos
   * Parametros: int id_identificao - identificador do roteador
   * Retorno: nenhum
   ********************************************************************************************/ 
   public Roteador(int id_identificao) {
        this.mutex = new  Semaphore(1);
        this.fila = new Fila();
        this.mutexFila = new Semaphore(0);
        this.id_identificao = id_identificao;
        this.HashTable = new Mapeamento(5);//define o tamanho da tabela 
        this.preencherMapeamento();
    } 
   
   /********************************************************************************************
   * Metodo: run
   * Funcao: iniciar a execucao da thread do roteador
   * Parametros: nenhum
   * Retorno: nenhum
   ********************************************************************************************/
   @Override
   public void run() {
         System.out.println("Roteador " + id_identificao + " inicializado");
         try { 
           roteamentoVetorDistancia();
         } catch(Exception ex) {
            ex.printStackTrace();
         }
   }//fim do metodo run 

   /********************************************************************************************
   * Metodo: roteamentoVetorDistancia
   * Funcao: gerenciar o roteamento do pacote pela camada de rede. Essa funcao eh composta por varias camada. Primeiro, ela gera varios pacotes de controle em que requri um medicao de custo ate cada um de seus vizinhos. Apos esse processo, o roteador envia peridicamente sua tabela de roteamento para cada um de seus vizinhos. E recebe essa a tabela de controle de cada um de seus vizinho, entao atualiza sua tabela usando o algoritimo de relaxamente de belaman-ford 
   * Parametros: nenhum
   * Retorno: nenhum
   ********************************************************************************************/
   private void roteamentoVetorDistancia() throws Exception {//essa função trabalha para fazer o prenchimento da tabela
        Pacote pacote = null;
        EstadoDeEventos Evento = EstadoDeEventos.inicio;
        while(true) {
            switch(Evento) {
                 case inicio:
                   pacote = new Pacote(Pacote.TipoPacote.ping_gerado);//inicia o pacote ping a ser enviado
                   pacote.setRoteador(this);//define o roteador atual como emissor de origem                                     
                   tabela.inserirLinha(id_identificao, null, 0);
                   enfileirarPacote(pacote);  //enfileira o pacote na fila
                   contadorTempo(240); //inicia o contador para enviar o pacote que conte dados da tabela, com um tempo suficientimento alto para que as tabeelas estejam preenchidas, caso nao estejam prenchidas, pior cenario, por raao de falhas tecnicas, o algoritmo ainda deve funcionar perfeitamente 

                   Evento = Evento.pacoteChegou;
                 break;   
                 case ProcessarPacote:
                   processarEnvio(pacote);//processo o pacote recebido com base no seu tipo. ping_enviado, envia um pacote de controle que contem o custo de seus vizinhos. Ping_back, descobre a informacao de cada um de seus vizinhos. Controle de tabela - atualiza sua tabela atual, enviar_tabela - propaga seu custo ate cada um de seus vizinhos. Ping-gerado, manda um pacote para estimar o custo de acesso ao enlance de seus vizinhos
                   Evento = EstadoDeEventos.pacoteChegou;  
                 break;
                 case pacoteChegou:
                    DownMutex(mutexFila);//mutex de controle de acesso ao buffer compattilhado limitado
                    DownMutex(mutex);//mutex de acesso a regiao critica, fundamental para evitar cndicao de corrida e estados incosistentes
                    try {
                      pacote = fila.desenfileirar();             
                    } catch(Exception ex) {
                      ex.printStackTrace();
                    } finally {
                       UpMutex(mutex);
                    }  
                    Evento = Evento.ProcessarPacote;
                 break; 
            }
        }
   }//fim do metodo roteamentoVetorDistancia


   /********************************************************************************************
   * Metodo: processarEnvio
   * Funcao: Para cada pacote recebido, o comportamento variara dependo da informacao que contem o tipo de pacote
   * Parametros: nenhum
   * Retorno: nenhum
   ********************************************************************************************/
   private void processarEnvio(Pacote pacote) throws Exception {
          Pacote.TipoPacote tipoPacote = pacote.getTipo();
          HashTable.getFuncao(tipoPacote.tipoAtual).funcao(pacote);    
   }//fim do metodo processarEnvio

   /********************************************************************************************
   * Metodo: preencherMapeamento
   * Funcao: um hash map de alta ordem manual. Veja como uma funcao de associacao H : {ping_gerado, ping_enviado, pingBack, enviar_Tabela, controleTabela} -> {funcaopropagarParaAdjacentes, funcaoenviarparaorigem, funcaoatualizartabela, funcaoprenchervizinhos}
   * Parametros: nenhum
   * Retorno: nenhum
   ********************************************************************************************/
   private void preencherMapeamento() {
         HashTable.associarFuncaoPosicao(Pacote.TipoPacote.ping_gerado, (pacoteParametro) -> {
                
                propagarParaAdjacentes(pacoteParametro, pacoteParametro.definirTipo(Pacote.TipoPacote.ping_enviado));
         });
         HashTable.associarFuncaoPosicao(Pacote.TipoPacote.ping_enviado, (pacoteParametro) -> {
                do {
                     if(interfaceSaida.getRoteadorAtual(id_identificao) == pacoteParametro.roteadorAnterior()) {
                           Pacote pacoteEnvio = new Pacote(Pacote.TipoPacote.pingBack);//informacao de controle que registra aquele enlace como pacote de retorno da requisicao anteirmente solicitada
                           pacoteEnvio.setRoteador(this);
                           pacoteEnvio.setRoteadorOrigem(this);
                           pacoteEnvio.definirCusto(interfaceSaida.getPesoAtual());
                           CordenarEnvio enviar =  new CordenarEnvio(ponteiro, 0); //zero indica que o envio se dara para controle de pacote
                           enviar.enviarPacote(this, interfaceSaida.getRoteadorAtual(id_identificao), pacoteEnvio);
                           enviar.start(); 
                     }
                     controlarEnvio();  
               } while(interfaceSaida.next());
         });
         HashTable.associarFuncaoPosicao(Pacote.TipoPacote.pingBack, (pacoteParametro) -> {
                tabela.definirHash(pacoteParametro.roteadorAnterior().getId_identificao(), pacoteParametro.obterCusto());
                tabela.inserirLinha(pacoteParametro.roteadorAnterior().getId_identificao(), pacoteParametro.getEmissor().getId_identificao(), pacoteParametro.obterCusto());
                ponteiro.atualizarTabela(tabela.mostrarTabela(), id_identificao);
                
                System.out.println(tabela.mostrarTabelaNoTermianl());
         });
         HashTable.associarFuncaoPosicao(Pacote.TipoPacote.enviar_Tabela, (pacoteParametro) -> {
                propagarParaAdjacentes(pacoteParametro, pacoteParametro.definirTipo(Pacote.TipoPacote.controleTabela));
                contadorTempo(240);
         });
         HashTable.associarFuncaoPosicao(Pacote.TipoPacote.controleTabela, (pacoteParametro) -> {
                
                 tabela.relaxacaoDasEntradas(pacoteParametro.getTabela(), pacoteParametro.roteadorAnterior().getId_identificao());
                 ponteiro.atualizarTabela(tabela.mostrarTabela(), id_identificao);
                 System.out.println(tabela.mostrarTabelaNoTermianl());
         });       
   }//fim do metodo preencherMapeamento

   /********************************************************************************************
   * Metodo: encaminhamentoMenorRota
   * Funcao: encaminha o pacote para a proxima rota baseando-se na consulta da tabela, veja que essa funcao independe do protocolo implementado, ja que o objetivo de todos os protocolos da rede eh determinar a arvore de escoamente de caminhos minimos sobre o grafo que modela a rede. Dada essa arvore, entao podemos realizar a composicao da tabela
   * Parametros: pacote
   * Retorno: nenhum
   ********************************************************************************************/
   public void encaminhamentoMenorRota(Pacote pacote) {
      Integer proximoHop = tabela.obterSaida(pacote.getIdRecptor());
      boolean controleQuadaEnlace = false;
      try { 
             if(proximoHop == null) {//se o proximo hop eh nulo, entao o caminho foi encontrado
                 System.out.println("caminho encontrado");
             } else if(proximoHop != null && proximoHop != -1) {//se o proximo hop eh diferente de nulo e -1 entao temos que esse hop objetivo ainda nao foi alcancado pela rede 
                 
                 do {
                       if(interfaceSaida.getRoteadorAtual(id_identificao).getId_identificao() == proximoHop) {
                          
                          Pacote pacoteEnvio = new Pacote(pacote.getTipo().tipoAtual);//determina de que o pacote atual eh um pacote de encaminhamento na rota
                          pacoteEnvio.definirTTL(pacote.getTTL());
                          pacoteEnvio.definirIdemissorReceptor(pacote.getIdEmissor(), pacote.getIdRecptor());
                          pacoteEnvio.setRoteador(pacote.roteadorAnterior()); 
                          if(verificarSePacoteEhValido(pacoteEnvio, proximoHop)) {
                             pacoteEnvio.setRoteador(this);
                             CordenarEnvio enviar =  new CordenarEnvio(ponteiro, 1); // 1 indica de que o encaminhamento sera orientado a tabela de roteamento
                             enviar.enviarPacote(this, interfaceSaida.getRoteadorAtual(id_identificao), pacoteEnvio);
                             enviar.start(); 
                             controlarEnvio();
                          } else {
                            System.out.println("caminho nao encontrado");
                            
                            proximoHop = -1;
                            ponteiro.emitirAviso();
                          }
                          controleQuadaEnlace = true;
                       }   
                 } while(interfaceSaida.next());
             } else {
                System.out.println("caminho nao encontrado");
                ponteiro.emitirAviso();     
             }
      } catch(Exception ex) {  
            
              proximoHop = null;  
      } finally {
           if(proximoHop == null || proximoHop == -1 || !controleQuadaEnlace) {
              SicronizacaoLeitorEscritor.deixarBaseDadosEscritor();//apos o caminho ser enontrado, temos de que o semaforo de acesso ao livro sera liberado  
              ponteiro.reabilitarButao();
           }
      } 
   }//fim do metodo encaminhamentoMenorRota

   /********************************************************************************************
   * Metodo: verificarSePacoteEhValido
   * Funcao: mecainismo formal para evitar loop infinito de pacote na rede
   * Parametros: pacote
   * Retorno: nenhum
   ********************************************************************************************/
   private boolean verificarSePacoteEhValido(Pacote pacote, Integer id_proximo_hop) {
           
           if(pacote.getTTL() <= 0) {
                return false; 
           }
           Integer id_anterior = pacote.roteadorAnterior().getId_identificao();
           
           if(id_anterior == id_proximo_hop) {
                 pacote.decrementarTTL();
           } else {
                 pacote.renovarTTL();  
           }
           return true;
   }//fim do metodo verificarSePacoteEhValido

   /********************************************************************************************
   * Metodo: propagarParaAdjacentes
   * Funcao: propaga os pacotes para os vizinhos adjacentes
   * Parametros: pacote, tipo
   * Retorno: nenhum
   ********************************************************************************************/
   private void propagarParaAdjacentes(Pacote pacote, Pacote.TipoPacote tipo) {
      try {   
        do {
                      Pacote pacoteEnvio = new Pacote(tipo.tipoAtual);//essa eh uma infomarcao de que esse pacote eh de controle, portanto enviando a informacao de volta que contem o custo de ida deste elance 
                      pacoteEnvio.setRoteador(pacote.roteadorAnterior());
                      if(tipo.tipoAtual == Pacote.TipoPacote.controleTabela) {
                         pacoteEnvio.setTabela(pacote.getTabela());
                      }
                      CordenarEnvio enviar =  new CordenarEnvio(ponteiro,0); //zero indica de que o pacote se dara para controle de preenchimento da tabela
                      enviar.enviarPacote(this, interfaceSaida.getRoteadorAtual(id_identificao), pacoteEnvio);
                      enviar.start(); 
                      controlarEnvio();
        } while(interfaceSaida.next());
      } catch(Exception ex) {
      }
        
   }//fim do metodo propagarParaAdjacentes
   
   private void checarId(Node<Roteador, Posicao> next_hop) {
         if(next_hop == null) {
              ponteiro.reabilitarButao();
         }
   }
   
 
  /********************************************************************************************
   * Metodo: iniciarEnvio
   * Funcao: iniciar o envio de um pacote com TTL definido
   * Parametros: nenhum
   * Retorno: nenhum
   ********************************************************************************************/
   public void iniciarEnvio(Pacote pacote) {
       enfileirarPacote(pacote);
   }
   
   /********************************************************************************************
   * Metodo: enfileirarPacote
   * Funcao: inserir pacote na fila com controle de concorrencia
   * Parametros: Pacote pacote
   * Retorno: nenhum
   ********************************************************************************************/
   public void enfileirarPacote(Pacote pacote) {    
           try { 
            DownMutex(mutex);
             if(!fila.cheia()) {
                 fila.enfileirar(pacote);
                 UpMutex(mutex);
                 UpMutex(mutexFila);
             } else {
               UpMutex(mutex);
             }
           } catch(Exception ex) {
              ex.printStackTrace();
           }
   }
   
   
   public void sinalizarEventoDeChegada(Pacote pacote) {
           enfileirarPacote(pacote);
   }


   public void definirAdjacencia(RoteadoresAdjacentes interfaceSaida) {
           this.tabela = new TabelaDistancia(id_identificao, interfaceSaida);
           this.interfaceSaida = interfaceSaida;
   }
  
  /********************************************************************************************
   * Metodo: DownMutex
   * Funcao: adquirir o semaforo
   * Parametros: Semaphore semaphore
   * Retorno: nenhum
   ********************************************************************************************/
  private void DownMutex(Semaphore semaphore) {
         try {
               semaphore.acquire();
         } catch(Exception ex) {
               ex.printStackTrace();
         }
   }
   
   /********************************************************************************************
   * Metodo: UpMutex
   * Funcao: liberar o semaforo
   * Parametros: Semaphore semaphore
   * Retorno: nenhum
   ********************************************************************************************/
   private void UpMutex(Semaphore semaphore) {
       try {
              semaphore.release();
         } catch(Exception ex) {
               ex.printStackTrace();
         }
   }

   private void contadorTempo(int tempoparametro) {
        final int tempoLambda = tempoparametro;
        new Thread(() -> {
            int tempo = tempoLambda;
            while(tempo > 0) {
               tempo--;
               try {
                  Thread.sleep(16);
               } catch (InterruptedException ex) {
                  ex.printStackTrace(); 
               }
           }
           Pacote pacote = new Pacote(Pacote.TipoPacote.enviar_Tabela);  
           pacote.setRoteador(this); 
           pacote.setTabela(tabela);
           enfileirarPacote(pacote);  
        }).start();
   }

   public int getId_identificao() {
         return id_identificao;
   }

   public void setPosicao(Posicao xs) {
          this.localizacao = xs;
   }

   public Posicao getPosicao() {
          return localizacao;
   }

   public void setControllerDeMover(ControllerDeMover ponteiro) {
          this.ponteiro = ponteiro;
   } 


   public void controlarEnvio() {
        try {
            Thread.sleep(10);
        } catch(Exception ex) {
            ex.printStackTrace();
        }
   }
}
