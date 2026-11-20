package servidor;

import comum.Comando;
import comum.Datagrama;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Servidor central do sistema de arquivos via UDP.
 * Concorrência: uma thread por cliente, criada quando o comando REGISTER é
 * recebido. A responsabilidade de transferência fica na ClienteHandler e o
 * Servidor se mantém livre para aceitar novos clientes.
 * Notificação em tempo real: a cada alteração na lista de arquivos o servidor
 * envia UPDATE_LIST para todos os clientes ativos.
 */
public class Servidor {

    public static final int PORTA = 5000;

    private static final int TAMANHO_BUFFER = 8192;

    /** Contador atômico para gerar ids de transferência únicos (int). */
    private final AtomicInteger CONTADOR_TRANSFERENCIAS = new AtomicInteger(1);

    private final DatagramSocket FANTOCHE;

    private final GerenciadorArquivos GERENCIADOR_DE_ARQUIVOS;

    private final LogServidor LOG;

    private final Map<InetSocketAddress, ClienteHandler> CLIENTES_ATIVOS = new ConcurrentHashMap<>();

    private volatile boolean executando = true;



    public Servidor () throws IOException {

        this(PORTA);
    }

    
    
    public Servidor (int porta) throws IOException {
        
        FANTOCHE = new DatagramSocket(porta);
        
        GERENCIADOR_DE_ARQUIVOS = new GerenciadorArquivos();
        
        LOG = new LogServidor();
    }



    public static void main (String[] args) {
        
        int porta = PORTA;
        
        if (args.length > 0) {
            
            try { porta = Integer.parseInt(args[0]); }
            
            catch (NumberFormatException e) { porta = PORTA; }
        }

        Servidor servidor;
        
        try { servidor = new Servidor(porta); }
        
        catch (Exception e) {
        
            System.err.println("[Servidor] Não foi possível abrir a porta " + porta + ": " + e.getMessage());
        
            System.exit(1);
        
            return;
        }

        servidor.iniciar();
    }
    
    

    public void iniciar () {
        
        LOG.registrar("", "SERVIDOR", "INICIO", "Aguardando registros na porta " + FANTOCHE.getLocalPort());
        
        System.out.println("[Servidor] Aguardando registros na porta " + FANTOCHE.getLocalPort());

        while (executando) {
            
            byte[] buffer = new byte[TAMANHO_BUFFER];
            
            DatagramPacket pacote = new DatagramPacket(buffer, buffer.length);
            
            try { FANTOCHE.receive(pacote); }
            
            catch (IOException e) {
            
                if (!executando) break;
            
                LOG.registrar("", "SERVIDOR", "ERRO", "Falha ao receber pacote: " + e.getMessage());
            
                continue;
            }
            
            tratarPacoteRecebido(pacote);
        }

        FANTOCHE.close();
        
        LOG.registrar("", "SERVIDOR", "ENCERRAMENTO", "Servidor encerrado.");
    }

    
    
    private void tratarPacoteRecebido (DatagramPacket pacote) {
        
        InetSocketAddress remetente = (InetSocketAddress) pacote.getSocketAddress();

        Datagrama datagrama;
        
        try {
            byte[] dados = Arrays.copyOf(pacote.getData(), pacote.getLength());
            
            datagrama = Datagrama.parse(dados);
        }
        
        catch (Exception e) {
        
            LOG.registrar("", "SERVIDOR", "ERRO", "Datagrama invalido de " + remetente + ": " + e.getMessage());
        
            return;
        }

        // Apenas REGISTER é tratado no loop principal.
        if (datagrama.getComando() != Comando.REGISTER) {
            
            if (!CLIENTES_ATIVOS.containsKey(remetente)) enviarErro(remetente, datagrama, "Cliente nao registrado. Envie REGISTER primeiro.");
            
            return;
        }

        if (CLIENTES_ATIVOS.containsKey(remetente)) {
            
            enviarAck(remetente, datagrama.getIdCliente());
            
            LOG.registrar("", "REGISTER", "DUPLICADO", "Cliente " + remetente + " ja registrado.");
            
            return;
        }

        // Extrai InetAddress e porta separados para o construtor do ClienteHandler.

        try {
            ClienteHandler handler = new ClienteHandler(
                    FANTOCHE,
                    remetente.getAddress(),
                    remetente.getPort(),
                    GERENCIADOR_DE_ARQUIVOS,
                    LOG,
                    this,
                    datagrama.getIdCliente()
            );

            CLIENTES_ATIVOS.put(remetente, handler);

            Thread threadHandler = new Thread(handler, "ClienteHandler-" + remetente);

            threadHandler.setDaemon(true);

            threadHandler.start();
        }

        catch (Exception e) {

            LOG.registrar("", "SERVIDOR", "ERRO", "Problema na criação do ClienteHandler: " + e.getMessage());

            return;
        }

        enviarAck(remetente, datagrama.getIdCliente());
        
        LOG.registrar("", "REGISTER", "OK", "Cliente registrado: " + remetente + " (id=" + datagrama.getIdCliente() + ")");
    }
    
    

    private void enviarAck (InetSocketAddress destino, String idCliente) {
        
        // ACK do REGISTER: idTransferencia=0, numeroFragmento=0.
        
        Datagrama ack = Datagrama.ack(0, 0);
        
        enviar(destino, ack, "ACK");
    }
    
    

    private void enviarErro (InetSocketAddress destino, Datagrama original, String mensagem) {
        
        Datagrama erro = Datagrama.erro(1, mensagem);
        
        enviar(destino, erro, "ERROR");
    }



    private void enviar (InetSocketAddress destino, Datagrama datagrama, String operacao) {
        
        try {
            byte[] dados = datagrama.serialize();
        
            FANTOCHE.send(new DatagramPacket(dados, dados.length, destino));
        }
        
        catch (Exception e) {
        
            LOG.registrar("", operacao, "ERRO", "Falha ao enviar para " + destino + ": " + e.getMessage());
        }
    }
    
    

    public void removerCliente (InetSocketAddress endereco) {
        
        ClienteHandler removido = CLIENTES_ATIVOS.remove(endereco);
        
        if (removido != null) LOG.registrar("", "CLIENTE", "REMOCAO", "Cliente removido: " + endereco);
    }
    
    

    public void notificarAtualizacaoLista () {

        try {
            List<String> lista = GERENCIADOR_DE_ARQUIVOS.listarArquivos();

            String listaSerializada = String.join("\n", lista == null ? new ArrayList<>() : lista);

            Datagrama atualizacao = Datagrama.atualizarLista(listaSerializada);

            byte[] dados = atualizacao.serialize();
            
            for (InetSocketAddress cliente : new ArrayList<>(CLIENTES_ATIVOS.keySet())) {
                
                try {
                    FANTOCHE.send(new DatagramPacket(dados, dados.length, cliente));
                }
                
                catch (Exception e) {
                
                    LOG.registrar("", "UPDATE_LIST", "AVISO", "Falha ao enviar lista para " + cliente + ": " + e.getMessage());
                }
            }
        }
        
        catch (Exception e) {
        
            LOG.registrar("", "UPDATE_LIST", "ERRO", "Falha ao serializar lista: " + e.getMessage());
        }

        LOG.registrar("", "UPDATE_LIST", "OK", "Lista enviada para " + CLIENTES_ATIVOS.size() + " cliente(s).");
    }
    
    

    /** Gera um id de transferência único (int) para uploads/downloads. */
    public int novoIdTransferencia () {
    
        return CONTADOR_TRANSFERENCIAS.getAndIncrement();
    }

    
    
    public void encerrar () {
    
        executando = false;
    
        FANTOCHE.close();
    }



    public DatagramSocket getFantoche () { return FANTOCHE; }
    
    public GerenciadorArquivos getGerenciadorArquivos () { return GERENCIADOR_DE_ARQUIVOS; }
    
    public LogServidor getLog () { return LOG; }
}