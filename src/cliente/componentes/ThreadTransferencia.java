package cliente.componentes;

import cliente.UDP.RespostaUDP;
import cliente.fantoches.SocketTransferencia;
import cliente.interfaceGrafica.Monitor;
import comum.Comando;
import comum.Datagrama;
import comum.Fragmento;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executa subir e baixar em segundo plano, fora da EDT do Swing.
 * Responsabilidades:
 *  - Fragmentar o arquivo em pedaços de 1024 bytes (MTU-safe);
 *  - Enviar/receber com ACK individual por fragmento;
 *  - Aplicar timeout de 2 s e retransmitir até 5 tentativas por fragmento;
 *  - Descartar fragmentos duplicados e bufferizar os fora de ordem;
 *  - Reportar progresso e resultado à interface exclusivamente via callback,
 *    sem tocar em componentes visuais.
 * A classe NÃO conhece Swing. Toda comunicação com a GUI passa pelo MONITOR.
 * Toda a comunicação usa o SocketTransferencia da transferência (porta própria); o servidor
 * identifica a operação pelo endereço de origem desse socket.
 */
public final class ThreadTransferencia extends Thread {

    private static final Logger LOGGER = Logger.getLogger(ThreadTransferencia.class.getName());

    /**
     * Tamanho do fragmento em bytes (1 KB, cabe no MTU Ethernet de 1500).
     */
    private static final int TAMANHO_FRAGMENTO = Fragmento.TAMANHO_MAXIMO; // 1024

    /**
     * Timeout de espera por resposta em milissegundos (2 s, adequado para LAN).
     */
    private static final int TIMEOUT_MS = 2000;

    /**
     * Limite de tentativas por fragmento/comando antes de abortar a transferência.
     */
    private static final int LIMITE_TENTATIVAS = 5;

//     estado

    private final Direcao DESCRICAO;

    private final Monitor MONITOR;

    private final InetSocketAddress SERVIDOR;

    private final SocketTransferencia MEIA_DE_TRANSFERENCIA;

//     Parâmetros de subir

    private final Path CAMINHO_ATE_ARQUIVO;

//     Parâmetros de baixar

    private final String NOME_ARQUIVO;

    private final Path DESTINO;

//     construção

    /**
     * Cria a thread da direção pedida.
     *
     * @param direcao     UPLOAD ou DOWNLOAD
     * @param servidor    endereço do servidor (IP + porta)
     * @param nomeArquivo nome do arquivo no servidor
     * @param caminhoLocal UPLOAD: arquivo local a enviar; DOWNLOAD: pasta onde o arquivo será salvo
     * @param monitor     callback para a interface gráfica
     * @param meia        socket dedicado a esta transferência
     */
    public static ThreadTransferencia criar
    (Direcao direcao, InetSocketAddress servidor, String nomeArquivo, Path caminhoLocal, Monitor monitor, SocketTransferencia meia) {

        if (direcao == null) throw new IllegalArgumentException("direcao não pode ser nula");

        return direcao == Direcao.UPLOAD
                ? new ThreadTransferencia(servidor, caminhoLocal, nomeArquivo, monitor, meia)
                : new ThreadTransferencia(servidor, nomeArquivo, caminhoLocal, monitor, meia);
    }



    /**
     * Construtor para UPLOAD.
     *
     * @param servidor       endereço do servidor (IP + porta)
     * @param caminhoArquivo arquivo local a ser enviado
     * @param nomeArquivo    nome do arquivo no servidor
     * @param monitor        callback para a interface gráfica
     * @param meia           socket dedicado a esta transferência
     */
    public ThreadTransferencia
    (InetSocketAddress servidor, Path caminhoArquivo, String nomeArquivo, Monitor monitor, SocketTransferencia meia) {

        super("ThreadTransferencia-UPLOAD");

        if (meia == null) throw new IllegalArgumentException("meia não pode ser nula");
        if (servidor == null) throw new IllegalArgumentException("servidor não pode ser nulo");
        if (caminhoArquivo == null) throw new IllegalArgumentException("CAMINHO_ATE_ARQUIVO não pode ser nulo");
        if (nomeArquivo == null || nomeArquivo.trim().isEmpty()) throw new IllegalArgumentException("NOME_ARQUIVO não pode ser vazio");
        if (monitor == null) throw new IllegalArgumentException("monitor não pode ser nulo");

        MEIA_DE_TRANSFERENCIA = meia;
        DESCRICAO = Direcao.UPLOAD;
        SERVIDOR = servidor;
        CAMINHO_ATE_ARQUIVO = caminhoArquivo;
        NOME_ARQUIVO = nomeArquivo;
        DESTINO = null;
        MONITOR = monitor;
    }



    /**
     * Construtor para DOWNLOAD.
     *
     * @param servidor         endereço do servidor (IP + porta)
     * @param nomeArquivo      nome do arquivo a baixar
     * @param diretorioDestino pasta onde o arquivo será salvo
     * @param monitor          callback para a interface gráfica
     * @param meia             socket dedicado a esta transferência
     */
    public ThreadTransferencia
    (InetSocketAddress servidor, String nomeArquivo, Path diretorioDestino, Monitor monitor, SocketTransferencia meia) {

        super("ThreadTransferencia-DOWNLOAD");

        if (meia == null) throw new IllegalArgumentException("meia não pode ser nula");
        if (servidor == null) throw new IllegalArgumentException("servidor não pode ser nulo");
        if (nomeArquivo == null || nomeArquivo.trim().isEmpty()) throw new IllegalArgumentException("NOME_ARQUIVO não pode ser vazio");
        if (diretorioDestino == null) throw new IllegalArgumentException("diretorioDestino não pode ser nulo");
        if (monitor == null) throw new IllegalArgumentException("monitor não pode ser nulo");

        MEIA_DE_TRANSFERENCIA = meia;
        DESCRICAO = Direcao.DOWNLOAD;
        SERVIDOR = servidor;
        CAMINHO_ATE_ARQUIVO = null;
        NOME_ARQUIVO = nomeArquivo;
        DESTINO = diretorioDestino;
        MONITOR = monitor;
    }



    private static byte[] remontar (byte[][] buffer, long tamanhoEsperado) throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream((int) tamanhoEsperado);

        for (byte[] frag : buffer) {

            if (frag == null) throw new IOException("Fragmento ausente na remontagem.");

            baos.write(frag);
        }

        byte[] resultado = baos.toByteArray();

        if (resultado.length != tamanhoEsperado) throw new IOException("Tamanho remontado (" + resultado.length + ") diverge do esperado (" + tamanhoEsperado + ").");

        return resultado;
    }



    @Override public void run () {

        try {

            if (DESCRICAO == Direcao.UPLOAD) subir();

            else baixar();
        }

        catch (IOException e) {

            LOGGER.log(Level.FINE, "Erro de I/O na transferência", e);

            MONITOR.onErro("Erro de rede: " + e.getMessage());
        }

        catch (Exception e) {

            LOGGER.log(Level.SEVERE, "Falha inesperada na transferência", e);

            MONITOR.onErro("Falha inesperada: " + e.getMessage());
        }
    }



//     UPLOAD

    /**
     * Fluxo de subir:
     * 1. Lê o arquivo e calcula o hash SHA-256;
     * 2. Envia UPLOAD com metadados (nome, tamanho, nº fragmentos, hash);
     * 3. Aguarda ACK do servidor, que devolve o idTransferencia;
     * 4. Para cada fragmento, envia DATA e aguarda ACK individual,
     * retransmitindo em caso de timeout (até 5 tentativas);
     * 5. Notifica progresso a cada ACK recebido;
     * 6. Aguarda o ACK de conclusão: o servidor só o envia depois de remontar o arquivo,
     * conferir o hash e gravar; falhas do servidor chegam como ERROR.
     */
    private void subir () throws IOException {

//      Leitura e fragmentação
        byte[] conteudo = Files.readAllBytes(CAMINHO_ATE_ARQUIVO);

        String hash = Datagrama.calcularHashSha256(conteudo);

        int numeroFragmentos = calcularNumeroFragmentos(conteudo.length);

        MONITOR.onStatus("Preparando envio de " + NOME_ARQUIVO + "...");

//      Envia arquivo subido, com metadados
        Datagrama upload = Datagrama.subir(NOME_ARQUIVO, conteudo.length, numeroFragmentos, hash);

//      Aguarda ACK do arquivo subido (retorna o idTransferencia atribuído pelo servidor)
        int idTransferencia = aguardarAckComando(upload);

//      Envia fragmentos um a um, com ACK individual e retransmissão
        Datagrama ultimoFragmento = null;

        for (int i = 0; i < numeroFragmentos; i++) {

            ultimoFragmento = Datagrama.dados(idTransferencia, i, extrairPayload(conteudo, i));

            enviarFragmentoComRetentativa(ultimoFragmento, idTransferencia, i);

//         documentação do progresso
            double fracao = (double) (i + 1) / numeroFragmentos;

            MONITOR.onProgresso(fracao);

            MONITOR.onStatus("Enviando fragmento " + (i + 1) + " de " + numeroFragmentos);
        }

        MONITOR.onStatus("Aguardando a confirmação final do servidor...");

        aguardarConclusaoDoUpload(idTransferencia, ultimoFragmento);

        MONITOR.onConcluido("Upload concluído: " + NOME_ARQUIVO);
    }



    /**
     * Envia um comando (UPLOAD ou DOWNLOAD) e aguarda ACK com retransmissão.
     * Retorna o idTransferencia informado pelo servidor no ACK.
     * Lança IOException se esgotar as tentativas ou se o servidor responder ERROR.
     */
    private int aguardarAckComando (Datagrama comando) throws IOException {

        for (int tentativa = 1; tentativa <= LIMITE_TENTATIVAS; tentativa++) {

            MEIA_DE_TRANSFERENCIA.enviar(comando, SERVIDOR);

            Datagrama resposta = aguardar(d -> d.getComando() == Comando.ERROR || d.getComando() == Comando.ACK);

            if (resposta == null) {

                LOGGER.info("Timeout aguardando ACK (tentativa " + tentativa + "/" + LIMITE_TENTATIVAS + ")");

                continue;
            }

            if (resposta.getComando() == Comando.ERROR) throw new IOException("Servidor recusou: " + resposta.getMensagemErro());

            return resposta.getIdTransferencia();
        }

        throw new IOException("Servidor não respondeu após " + LIMITE_TENTATIVAS + " tentativas.");
    }



    /**
     * Aguarda o ACK de conclusão (numeroFragmento = -1) ou um ERROR do servidor.
     * Se o tempo esgotar, repete o último DATA: o servidor reconfirma esse fragmento e reenvia a conclusão.
     */
    private void aguardarConclusaoDoUpload (int idTransferencia, Datagrama ultimoFragmento) throws IOException {

        for (int tentativa = 1; tentativa <= LIMITE_TENTATIVAS; tentativa++) {

            Datagrama resposta = aguardar(d -> d.getComando() == Comando.ERROR
                    || (d.getComando() == Comando.ACK && d.getIdTransferencia() == idTransferencia && d.getNumeroFragmento() == -1));

            if (resposta != null) {

                if (resposta.getComando() == Comando.ERROR) throw new IOException("Servidor recusou o arquivo: " + resposta.getMensagemErro());

                return;
            }

            LOGGER.info("Timeout aguardando a conclusão do upload (tentativa " + tentativa + "/" + LIMITE_TENTATIVAS + ")");

            if (ultimoFragmento != null) MEIA_DE_TRANSFERENCIA.enviar(ultimoFragmento, SERVIDOR);
        }

        throw new IOException("O servidor não confirmou a conclusão do upload após " + LIMITE_TENTATIVAS + " tentativas.");
    }



//     DOWNLOAD

    /**
     * Fluxo de baixar:
     * 1. Envia DOWNLOAD com o nome do arquivo;
     * 2. Aguarda metadados (servidor responde com UPLOAD contendo nome, tamanho,
     * nº fragmentos, hash e id da transferência);
     * 3. Recebe os fragmentos DATA, enviando ACK individual a cada um;
     * fragmentos duplicados são descartados (e reconfirmados), fora de ordem são bufferizados;
     * 4. Ao completar, remonta o arquivo, confere o hash e salva no destino;
     * 5. Notifica progresso a cada fragmento confirmado.
     */
    private void baixar () throws IOException {

//      Requisição
        MONITOR.onStatus("Solicitando download de " + NOME_ARQUIVO + "...");

        Datagrama download = Datagrama.baixar(NOME_ARQUIVO);

//      Aguarda metadados (servidor responde com UPLOAD contendo nome, tamanho, nº fragmentos, hash)
        MetadadosDownload meta = aguardarMetadadosDownload(download);

//         Buffer de fragmentos recebidos (indexado por numeroFragmento)
        byte[][] buffer = new byte[meta.numeroFragmentos()][];

        int recebidos = 0;

        int esperasConsecutivas = 0;

        while (recebidos < meta.numeroFragmentos()) {

            RespostaUDP r = MEIA_DE_TRANSFERENCIA.receber(TIMEOUT_MS);

            if (r.esgotado()) {

//              Timeout sem receber nada: quem retransmite é o servidor. O cliente aguarda,
//              até o limite de esperas, e então considera o servidor indisponível.
                esperasConsecutivas++;

                if (esperasConsecutivas >= LIMITE_TENTATIVAS) throw new IOException("O servidor deixou de enviar fragmentos (" + LIMITE_TENTATIVAS + " esperas de " + (TIMEOUT_MS / 1000) + " s sem resposta).");

                continue;
            }

            esperasConsecutivas = 0;

            Datagrama resposta = r.datagrama();

            if (resposta.getComando() == Comando.ERROR) throw new IOException("Servidor reportou erro no download: " + resposta.getMensagemErro());

            if (resposta.getComando() != Comando.DATA) continue;

            int numFrag = resposta.getNumeroFragmento();

            int idTrans = resposta.getIdTransferencia();

//          Valida se pertence a esta transferência
            if (idTrans != meta.idTransferencia()) {

                LOGGER.warning("DATA com idTransferencia inesperado: " + idTrans);

                continue;
            }

//          Valida número do fragmento
            if (numFrag < 0 || numFrag >= meta.numeroFragmentos()) {

                LOGGER.warning("DATA com numeroFragmento fora do intervalo: " + numFrag);

                continue;
            }

//          Descarta duplicados, mas reconfirma: o ACK anterior pode ter se perdido
            if (buffer[numFrag] != null) {

                LOGGER.fine("Fragmento duplicado descartado: " + numFrag);

                MEIA_DE_TRANSFERENCIA.enviar(Datagrama.ack(meta.idTransferencia(), numFrag), SERVIDOR);

                continue;
            }

//          Bufferiza (fora de ordem ou não — a remontagem acontece no final)
            buffer[numFrag] = resposta.getPayload();

            recebidos++;

//          Envia ACK individual
            MEIA_DE_TRANSFERENCIA.enviar(Datagrama.ack(meta.idTransferencia(), numFrag), SERVIDOR);

//          Progresso
            double fracao = (double) recebidos / meta.numeroFragmentos();

            MONITOR.onProgresso(fracao);

            MONITOR.onStatus("Recebendo fragmento " + recebidos + " de " + meta.numeroFragmentos());
        }

//      Espera a conclusão do servidor, reconfirmando fragmentos repetidos se o último ACK se perdeu
        aguardarConclusaoDoDownload(meta.idTransferencia());

//      Remonta o arquivo
        byte[] conteudo = remontar(buffer, meta.tamanhoArquivo());

//      Confere o hash
        if (!Datagrama.hashConfere(conteudo, meta.hash())) throw new IOException("Hash SHA-256 divergente após a remontagem. Arquivo pode estar corrompido.");

//      Salva no destino (somente o nome final do arquivo é usado)
        Path destino = DESTINO.resolve(Path.of(NOME_ARQUIVO).getFileName().toString());

        Files.write(destino, conteudo);

        MONITOR.onConcluido("Download concluído: " + NOME_ARQUIVO);
    }



    /**
     * Aguarda o datagrama de metadados do baixar (UPLOAD do servidor).
     * Em caso de timeout, reenvia a requisição DOWNLOAD (até LIMITE_TENTATIVAS).
     */
    private MetadadosDownload aguardarMetadadosDownload (Datagrama download) throws IOException {

        for (int tentativa = 1; tentativa <= LIMITE_TENTATIVAS; tentativa++) {

            MEIA_DE_TRANSFERENCIA.enviar(download, SERVIDOR);

            Datagrama resp = aguardar(d -> d.getComando() == Comando.ERROR || d.getComando() == Comando.UPLOAD);

            if (resp == null) {

                LOGGER.info("Timeout aguardando metadados do download (tentativa " + tentativa + ")");

                continue;
            }

            if (resp.getComando() == Comando.ERROR) throw new IOException("Servidor recusou o download: " + resp.getMensagemErro());

//          O servidor reutiliza o layout de UPLOAD para enviar metadados
//          (nome, tamanho, nº fragmentos, hash e idTransferencia).
            return new MetadadosDownload(
                    resp.getIdTransferencia(),
                    resp.getNome(),
                    resp.getTamanhoArquivo(),
                    resp.getNumeroFragmentos(),
                    resp.getHash()
            );
        }

        throw new IOException("Servidor não respondeu ao DOWNLOAD após " + LIMITE_TENTATIVAS + " tentativas.");
    }



    /**
     * Espera, por no máximo TIMEOUT_MS, o ACK de conclusão do download. O arquivo já está completo, então o tempo
     * esgotado ou um ERROR do servidor não impedem o fim. Durante a espera, fragmentos repetidos são reconfirmados.
     */
    private void aguardarConclusaoDoDownload (int idTransferencia) throws IOException {

        long prazo = System.currentTimeMillis() + TIMEOUT_MS;

        while (true) {

            long restante = prazo - System.currentTimeMillis();

            if (restante <= 0) return;

            RespostaUDP r = MEIA_DE_TRANSFERENCIA.receber((int) restante);

            if (r.esgotado()) return;

            Datagrama d = r.datagrama();

            if (d.getComando() == Comando.ACK && d.getIdTransferencia() == idTransferencia && d.getNumeroFragmento() == -1) return;

            if (d.getComando() == Comando.DATA && d.getIdTransferencia() == idTransferencia) {

                MEIA_DE_TRANSFERENCIA.enviar(Datagrama.ack(idTransferencia, d.getNumeroFragmento()), SERVIDOR);
            }
        }
    }



//     auxiliares

    /**
     * Espera, por no máximo TIMEOUT_MS no total, um datagrama aceito pelo filtro.
     * Datagramas que o filtro recusa (ACK atrasado, por exemplo) são ignorados sem consumir uma tentativa.
     *
     * @return o datagrama aceito, ou {@code null} se o tempo esgotar.
     */
    private Datagrama aguardar (Predicate<Datagrama> aceito) throws IOException {

        long prazo = System.currentTimeMillis() + TIMEOUT_MS;

        while (true) {

            long restante = prazo - System.currentTimeMillis();

            if (restante <= 0) return null;

            RespostaUDP r = MEIA_DE_TRANSFERENCIA.receber((int) restante);

            if (r.esgotado()) return null;

            Datagrama d = r.datagrama();

            if (aceito.test(d)) return d;

            LOGGER.fine("Datagrama inesperado ignorado: " + d.getComando());
        }
    }



    /**
     * Envia um fragmento DATA e aguarda o ACK individual.
     * Retransmite em caso de timeout, até LIMITE_TENTATIVAS tentativas.
     * Lança IOException se esgotar as tentativas.
     */
    private void enviarFragmentoComRetentativa (Datagrama data, int idTransferencia, int numeroFragmento) throws IOException {

        for (int tentativa = 1; tentativa <= LIMITE_TENTATIVAS; tentativa++) {

            MEIA_DE_TRANSFERENCIA.enviar(data, SERVIDOR);

            Datagrama resp = aguardar(d -> d.getComando() == Comando.ERROR
                    || (d.getComando() == Comando.ACK && d.getIdTransferencia() == idTransferencia && d.getNumeroFragmento() == numeroFragmento));

            if (resp == null) {

                LOGGER.info("Timeout aguardando ACK do fragmento " + numeroFragmento
                        + " (tentativa " + tentativa + "/" + LIMITE_TENTATIVAS + ")");

                continue; // retransmite
            }

            if (resp.getComando() == Comando.ERROR) throw new IOException("Servidor recusou fragmento " + numeroFragmento + ": " + resp.getMensagemErro());

            return; // ACK correto, fragmento confirmado
        }

        throw new IOException("Fragmento " + numeroFragmento + " não confirmado após " + LIMITE_TENTATIVAS + " tentativas. Transferência abortada.");
    }



    /**
     * Calcula quantos fragmentos de 1024 bytes são necessários para o arquivo.
     * Arquivos vazios geram 0 fragmentos (não faz sentido enviar).
     */
    private static int calcularNumeroFragmentos (long tamanhoArquivo) {

        if (tamanhoArquivo <= 0) return 0;

        return (int) Math.ceil((double) tamanhoArquivo / TAMANHO_FRAGMENTO);
    }



    /**
     * Extrai o payload do i-ésimo fragmento a partir do conteúdo completo.
     */
    private static byte[] extrairPayload (byte[] conteudo, int indiceFragmento) {

        int inicio = indiceFragmento * TAMANHO_FRAGMENTO;

        int fim = Math.min(inicio + TAMANHO_FRAGMENTO, conteudo.length);

        byte[] payload = new byte[fim - inicio];

        System.arraycopy(conteudo, inicio, payload, 0, payload.length);

        return payload;
    }
}
