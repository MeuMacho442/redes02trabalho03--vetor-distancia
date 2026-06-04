package Model;

public class Aresta<T,K> {
     int peso;
     Node<T, K> node;
     public Aresta(Node<T, K> node, int peso) {
          this.peso = peso;
          this.node = node;
     }
     
     public Node<T, K> getNode() {
        return node;
     }
     
     public int getPeso() {
        return peso;
     }
}
