package cliente;

import comum.Datagrama;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Arrays;

/**
 * Socket UDP dedicado às transferências de arquivo (subir/baixar).
 *
 * Ele é independente do FANTOCHE principal do cliente, usado pela thread de
 * escuta de UPDATE_LIST. Assim, a thread de transferência pode enviar e
 * receber ACKs sem disputar datagramas com a escuta.
 *
 * Reutiliza a classe {@link RespostaUDP} para manter o mesmo
 * contrato de resposta (timeout/dados) usado pelo restante do cliente.
 */
public final class SocketTransferencia implements AutoCloseable {

    /** Tamanho máximo de um datagrama UDP (payload IP - cabeçalhos UDP/IP). */
    private static final int BUFFER_TAMANHO = 65507;



    private final DatagramSocket FANTOCHE;

    /**
     * Abre um FANTOCHE UDP em porta efêmera, exclusivo para a transferência.
     */
    public SocketTransferencia() throws IOException {

        FANTOCHE = new DatagramSocket();
    }



    /**
     * Envia um datagrama para o servidor de transferência.
     */
    public void enviar(Datagrama datagrama, InetSocketAddress destino) throws IOException {

        if (datagrama == null || destino == null) throw new IllegalArgumentException("Datagrama e destino não podem ser nulos.");

        byte[] dados;

        try {
            dados = datagrama.serialize();
        }

        catch (Exception e) {

            throw new IOException("Falha ao serializar datagrama.", e);
        }

        DatagramPacket pacote = new DatagramPacket(dados, dados.length, destino);

        FANTOCHE.send(pacote);
    }

    
    
    /**
     * Aguarda uma resposta no FANTOCHE de transferência.
     *
     * @param timeoutMs tempo máximo de espera em milissegundos.
     * @return {@link RespostaUDP} com o datagrama ou indicando timeout.
     */
    public RespostaUDP receber(int timeoutMs) throws IOException {
    
        byte[] buffer = new byte[BUFFER_TAMANHO];
    
        DatagramPacket pacote = new DatagramPacket(buffer, buffer.length);

        FANTOCHE.setSoTimeout(timeoutMs);
    
        try {
            FANTOCHE.receive(pacote);
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
     * Devolve o endereço local (IP e porta efêmera) do FANTOCHE.
     */
    public InetSocketAddress obterEnderecoLocal() {
        return (InetSocketAddress) FANTOCHE.getLocalSocketAddress();
    }

    
    
    /**
     * Fecha o FANTOCHE de transferência.
     */
    public void fechar() {
        
        if (FANTOCHE != null && !FANTOCHE.isClosed()) FANTOCHE.close();
    }
    

    
    @Override public void close() {
        fechar();
    }
}
