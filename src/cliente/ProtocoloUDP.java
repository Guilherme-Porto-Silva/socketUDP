package cliente;

import comum.Datagrama;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Arrays;

/**
 * ProtocoloUDP encapsula o envio e a recepção de datagramas no lado do cliente.
 * 
 * Responsabilidades:
 * - Configurar e manter um DatagramSocket para o cliente;
 * - Enviar datagramas para o servidor;
 * - Receber respostas respeitando um timeout máximo;
 * - Devolver um indicador de timeout quando o servidor não responde,
 *   para que a interface e as threads de transferência tratem a indisponibilidade.
 * 
 * Esta classe NÃO monta nem interpreta os campos do protocolo: a serialização e o
 * parse são delegados aos métodos de Datagrama (contrato comum em src/comum).
 */
public class ProtocoloUDP {

    public static final int TIMEOUT_PADRAO_MS = 2000;
    
    public static final int BUFFER_TAMANHO = 65507;

    private final DatagramSocket MEIA;

    /**
     * Abre um DatagramSocket em porta efêmera, adequado para o lado cliente.
     */
    public ProtocoloUDP () throws IOException {
        
        MEIA = new DatagramSocket();
    }
    
    

    /**
     * Permite reaproveitar um DatagramSocket já criado, quando o chamador
     * precisar controlar o bind (por exemplo, em testes).
     */
    public ProtocoloUDP (DatagramSocket meia) {
        
        if (meia == null) throw new IllegalArgumentException("O DatagramSocket não pode ser nulo.");
        
        MEIA = meia;
    }
    
    

    /**
     * Encerra o fantoche e libera os recursos de rede.
     */
    public void fechar () {
        
        if (MEIA != null && !MEIA.isClosed()) MEIA.close();
    }

    
    
    /**
     * Envia um datagrama para o destino informado.
     * 
     * A serialização do datagrama é delegada a Datagrama.serializar().
     */
    public void enviar (Datagrama datagrama, InetSocketAddress destino) throws IOException {
        
        if (datagrama == null || destino == null) throw new IllegalArgumentException("Datagrama e destino não podem ser nulos.");

        byte[] dados;

        try {
            dados = datagrama.serialize();
        }
        
        catch (Exception e) {
        
            throw new IOException("Falha ao serializar datagrama para envio.", e);
        }

        DatagramPacket pacote = new DatagramPacket(dados, dados.length, destino);
        
        MEIA.send(pacote);
    }

    
    
    /**
     * Envia um datagrama para o endereço e porta indicados separadamente.
     */
    public void enviar (Datagrama datagrama, InetAddress endereco, int porta) throws IOException {
    
        enviar(datagrama, new InetSocketAddress(endereco, porta));
    }


    
    /**
     * Aguarda e recebe um datagrama dentro do tempo limite informado.
     * 
     * @param timeoutMs tempo máximo de espera em milissegundos.
     * @return RespostaUDP com o datagrama recebido e o endereço de origem,
     *         ou RespostaUDP.esgotado() verdadeiro quando o tempo acabar.
     */
    public RespostaUDP receber (int timeoutMs) throws IOException {
        
        byte[] buffer = new byte[BUFFER_TAMANHO];
        
        DatagramPacket pacote = new DatagramPacket(buffer, buffer.length);

        
        MEIA.setSoTimeout(timeoutMs);
        
        try {
            MEIA.receive(pacote);
        }
        
        catch (SocketTimeoutException e) {
        
            return RespostaUDP.esgotar();
        }

        byte[] dados = Arrays.copyOf(pacote.getData(), pacote.getLength());
        
        Datagrama datagrama;
        
        try {
            datagrama = Datagrama.parse(dados);
        }
        
        catch (Exception e) {
        
            throw new IOException("Falha ao interpretar datagrama recebido.", e);
        }
        
        InetSocketAddress origem = (InetSocketAddress) pacote.getSocketAddress();
        
        return RespostaUDP.dados(datagrama, origem);
    }
    
    

    /**
     * Envia um datagrama e aguarda a resposta dentro do timeout informado.
     * 
     * É o fluxo principal para comandos do cliente que esperam ACK,
     * ERROR, lista ou metadados do servidor.
     */
    public RespostaUDP enviarEReceber(Datagrama datagrama, InetSocketAddress destino, int timeoutMs) throws IOException {
        
        enviar(datagrama, destino);
        
        return receber(timeoutMs);
    }
    
    

    /**
     * Devolve o endereço local (IP e porta efêmera) usado pelo MEIA do cliente.
     */
    public InetSocketAddress obterEnderecoLocal() {
    
        return (InetSocketAddress) MEIA.getLocalSocketAddress();
    }
}