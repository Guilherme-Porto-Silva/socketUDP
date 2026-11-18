package servidor;

import comum.Datagrama;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Objects;

/**
 * Possui três estados
 * mutuamente exclusivos, acessíveis via {@link #getTipo()}:
 *
 * <ul>
 *   <li>{@code DATAGRAMA} — DATAGRAMA válido parseado; {@link #getDATAGRAMA()}
 *       e {@link #getORIGEM()} são não-nulos;</li>
 *   <li>{@code TIMEOUT} — o tempo de espera expirou sem chegada de dados;
 *       todos os demais campos são nulos;</li>
 *   <li>{@code MALFORMADO} — chegou um DATAGRAMA, mas o parse falhou;
 *       {@link #getORIGEM()}, {@link #getDadosBrutos()} e {@link #getMotivo()}
 *       são não-nulos.</li>
 * </ul>
 */
public final class ResultadoRecebimento {

    private final Tipo TIPO;
    
    private final Datagrama DATAGRAMA;
    
    private final InetSocketAddress ORIGEM;
    
    private final byte[] DADOS_BRUTOS;
    
    private final String MOTIVO;

    
    
    private ResultadoRecebimento(Tipo tipo, Datagrama datagrama, InetSocketAddress origem, byte[] dadosBrutos, String motivo) {

        TIPO = tipo;

        DATAGRAMA = datagrama;

        ORIGEM = origem;

        DADOS_BRUTOS = dadosBrutos;

        MOTIVO = motivo;
    }
    
    

    static ResultadoRecebimento esgotar () {
        return new ResultadoRecebimento(Tipo.TIMEOUT, null, null, null, null);
    }

    
    
    static ResultadoRecebimento datagrama (Datagrama datagrama, InetSocketAddress origem) {
        
        return new ResultadoRecebimento(Tipo.DATAGRAMA, Objects.requireNonNull(datagrama), Objects.requireNonNull(origem), null, null);
    }

    
    
    static ResultadoRecebimento malformado (InetSocketAddress origem, byte[] dadosBrutos, String motivo) {
    
        return new ResultadoRecebimento(Tipo.MALFORMADO, null, Objects.requireNonNull(origem), Objects.requireNonNull(dadosBrutos), Objects.requireNonNull(motivo));
    }



    public Tipo getTipo () {
        return TIPO;
    }

    /** Válido apenas quando {@code getTIPO() == DATAGRAMA}. */
    public Datagrama getDATAGRAMA () {
        return DATAGRAMA;
    }

    /** Válido quando {@code getTIPO() == DATAGRAMA} ou {@code MALFORMADO}. */
    public InetSocketAddress getORIGEM () {
        return ORIGEM;
    }

    /** Válido apenas quando {@code getTIPO() == MALFORMADO}; cópia defensiva. */
    public byte[] getDadosBrutos () {

        return DADOS_BRUTOS == null ? null : Arrays.copyOf(DADOS_BRUTOS, DADOS_BRUTOS.length);
    }

    /** Válido apenas quando {@code getTIPO() == MALFORMADO}. */
    public String getMotivo () {
        return MOTIVO;
    }
}