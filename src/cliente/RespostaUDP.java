package cliente;

import comum.Datagrama;
import java.net.InetSocketAddress;

/**
 * Resultado de uma chamada de recepção.
 *
 * Guarda o datagrama recebido e o endereço de ORIGEM, ou indica que o
 * tempo de espera terminou sem resposta do servidor.
 */
public record RespostaUDP (boolean esgotado, Datagrama datagrama, InetSocketAddress ORIGEM) {

    public static RespostaUDP esgotar () {

        return new RespostaUDP(true, null, null);
    }

    public static RespostaUDP dados (Datagrama datagrama, InetSocketAddress origem) {

        return new RespostaUDP(false, datagrama, origem);
    }

    /**
     * Informa se há um datagrama recebido.
     */
    public boolean temDados () { return !esgotado && datagrama != null; }
}